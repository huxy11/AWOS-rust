use quick_xml::{events::Event, Reader};

use std::{collections::HashSet, iter::FromIterator};

use oss_sdk::{OSSClient, SignAndDispatch};

use super::*;

impl<C: SignAndDispatch> AwosApi for OSSClient<C> {
    fn list_object<'a, O, R>(&self, opts: O) -> Result<R>
    where
        O: Into<Option<ListOptions<'a>>>,
        R: FromIterator<String>,
    {
        self.list_details(opts).map(|resp| resp.to_obj_names())
    }
    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>>,
        // R: FromIterator<String>,
    {
        let mut rqst = self.get_request(None);
        if let Some(_opts) = opts.into() {
            rqst.set_params(_opts.to_params().into_iter().collect());
        }
        let resp = self.sign_and_dispatch(rqst)?;

        if resp.status.is_success() {
            let resp_content = std::str::from_utf8(&resp.body)?;
            let mut reader = Reader::from_str(resp_content);
            let mut buf = Vec::new();
            let mut result = ListDetailsResp::default();
            let mut cur_obj = ObjectDetails::default();
            reader.trim_text(true);
            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Start(ref e)) => match e.name() {
                        b"Contents" => {}
                        b"Key" => cur_obj.key = reader.read_text(e.name(), &mut Vec::new())?,
                        b"LastModified" => {
                            cur_obj.last_modified = reader.read_text(e.name(), &mut Vec::new())?
                        }
                        b"ETag" => cur_obj.e_tag = reader.read_text(e.name(), &mut Vec::new())?,
                        b"Size" => cur_obj.size = reader.read_text(e.name(), &mut Vec::new())?,
                        b"IsTruncated" => {
                            result.is_truncated =
                                reader.read_text(e.name(), &mut Vec::new())?.parse()?
                        }
                        b"NextContinuationToken" => {
                            result.next_marker = reader.read_text(e.name(), &mut Vec::new())?
                        }
                        b"Prefixes" => {
                            result.prefixe = reader.read_text(e.name(), &mut Vec::new())?
                        }
                        _ => (),
                    },
                    Ok(Event::End(ref e)) => match e.name() {
                        b"Contents" => {
                            result.objects.push(std::mem::take(&mut cur_obj));
                        }
                        _ => (),
                    },
                    Ok(Event::Eof) => break,
                    Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                    _ => (),
                }
                buf.clear();
            }
            Ok(result)
        } else {
            Err(Error::Object(ObjectError::GetError {
                msg: resp_to_error(&resp),
            }))
        }
    }
    fn get<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<types::GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>,
    {
        Ok(self.get_as_buffer(key, meta_keys_filter)?.into())
    }

    fn get_as_buffer<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>,
    {
        let rqst = self.get_request(key.as_ref());
        let resp = self.sign_and_dispatch(rqst)?;
        if resp.status.is_success() {
            let mut get_resp: GetAsBufferResp = resp.into();
            if let Some(_meta_keys_filter) = meta_keys_filter.into() {
                get_resp.filter(_meta_keys_filter);
            }
            Ok(get_resp)
        } else {
            Err(Error::Object(ObjectError::GetError {
                msg: resp_to_error(&resp),
            }))
        }
    }
    fn head<S>(&self, key: S) -> Result<std::collections::HashMap<String, String>>
    where
        S: AsRef<str>,
    {
        let mut resp = self.get_as_buffer(key, None)?;
        resp.headers.extend(resp.meta.into_iter());
        Ok(resp.headers)
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<PutOrCopyOptions<'a>>>,
    {
        let mut rqst = self.put_request(key.as_ref(), data.into());
        if let Some(_opts) = opts.into() {
            rqst.add_headers(_opts.to_headers());
            rqst.add_meta(_opts.meta.into_iter().map(|item| item.to_owned()));
        }
        let resp = self.sign_and_dispatch(rqst)?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(Error::Object(ObjectError::PutError {
                msg: resp_to_error(&resp),
            }))
        }
    }

    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<PutOrCopyOptions<'a>>>,
    {
        let mut rqst = self.put_request(key.as_ref(), None);
        if let Some(_opts) = opts.into() {
            let mut headers = _opts.to_headers();
            headers.push(("x-oss-copy-source".to_owned(), src.into()));
            rqst.add_headers(headers);
            rqst.add_meta(_opts.meta.into_iter().map(|item| item.to_owned()));
        }
        let resp = self.sign_and_dispatch(rqst)?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(Error::Object(ObjectError::PutError {
                msg: resp_to_error(&resp),
            }))
        }
    }

    fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str>,
    {
        let rqst = self.del_request(key.as_ref());
        let resp = self.sign_and_dispatch(rqst)?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(Error::Object(ObjectError::DeleteError {
                msg: resp_to_error(&resp),
            }))
        }
    }

    fn del_multi<K, S>(&self, keys: K) -> Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        for key in keys.into_iter() {
            self.del(key)?;
        }
        Ok(())
    }

    // AWOS-JS 貌似省略了 content-type 和 content-md5。 这里也先省略
    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<SignUrlOptions<'a>>>,
    {
        let opts = opts.into().unwrap_or_default();
        let expires = opts.expires;
        let method = opts.method;
        Ok(self.get_signed_url(key.as_ref(), method, expires, "", None))
    }
}

fn resp_to_error(resp: &HttpResponse) -> String {
    format!(
        "SatusCode:{}\nContent:{}",
        resp.status,
        std::str::from_utf8(&resp.body.to_vec()).unwrap_or("Cannot stringify the content")
    )
}
