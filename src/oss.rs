use crate::{
    errors::{Error, ParseError},
    types, AwosApi, GetAsBufferResp, ListDetailsResp, ListOptions, ObjectDetails, PutOrCopyOptions,
    Result, SignedUrlOptions,
};

use async_trait::async_trait;

use oss_sdk::{OSSClient, SignAndDispatch};

use quick_xml::{events::Event, Reader};

#[async_trait]
impl<C: SignAndDispatch + Send + Sync> AwosApi for OSSClient<C> {
    async fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        self.list_details(opts)
            .await
            .map(|resp| resp.to_obj_names())
    }
    async fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        let mut rqst = self.get_request(None);
        if let Some(_opts) = opts.into() {
            rqst.set_params(_opts.to_params().into_iter().collect());
        }
        let resp = self.sign_and_dispatch(rqst).await?;

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
                            result.is_truncated = reader
                                .read_text(e.name(), &mut Vec::new())?
                                .parse()
                                .map_err(|_| {
                                    Error::Parse(ParseError::InvalidFormat {
                                        msg: format!("Failed parsing content IsTruncated to bool"),
                                    })
                                })?
                        }
                        b"NextContinuationToken" => {
                            result.next_marker = reader.read_text(e.name(), &mut Vec::new())?
                        }
                        b"Prefixes" => {
                            result.prefix = reader.read_text(e.name(), &mut Vec::new())?
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
            Err(resp.status.as_u16().into())
        }
    }
    async fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<types::GetResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        Ok(self.get_as_buffer(key, meta_keys_filter).await?.into())
    }

    async fn get_as_buffer<'a, S, M, F>(
        &self,
        key: S,
        meta_keys_filter: M,
    ) -> Result<GetAsBufferResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        let rqst = self.get_request(key.as_ref());
        let resp = self.sign_and_dispatch(rqst).await?;
        if resp.status.is_success() {
            let mut get_resp: GetAsBufferResp = resp.into();
            if let Some(_meta_keys_filter) = meta_keys_filter.into() {
                let _filter = _meta_keys_filter.into_iter().collect();
                get_resp.filter(_filter);
            }
            Ok(get_resp)
        } else {
            Err(resp.status.as_u16().into())
        }
    }
    async fn head<S>(&self, key: S) -> Result<std::collections::HashMap<String, String>>
    where
        S: AsRef<str> + Send,
    {
        let mut resp = self.get_as_buffer::<_, _, Vec<_>>(key, None).await?;
        resp.headers.extend(resp.meta.into_iter());
        Ok(resp.headers)
    }

    async fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str> + Send,
        D: Into<Box<[u8]>> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        let mut rqst = self.put_request(key.as_ref(), data.into());
        if let Some(_opts) = opts.into() {
            rqst.add_headers(_opts.as_headers());
            rqst.add_meta(_opts.meta.unwrap_or_default());
        }
        let resp = self.sign_and_dispatch(rqst).await?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(resp.status.as_u16().into())
        }
    }

    async fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String> + Send,
        S2: AsRef<str> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        let mut rqst = self.put_request(key.as_ref(), None);
        if let Some(_opts) = opts.into() {
            let mut headers = _opts.as_headers();
            let src = src.into();
            headers.insert("x-oss-copy-source", &src);
            rqst.add_headers(headers);
            rqst.add_meta(_opts.meta.unwrap_or_default());
        }
        let resp = self.sign_and_dispatch(rqst).await?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(resp.status.as_u16().into())
        }
    }

    async fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str> + Send,
    {
        let rqst = self.del_request(key.as_ref());
        let resp = self.sign_and_dispatch(rqst).await?;
        if resp.status.is_success() {
            Ok(())
        } else {
            Err(resp.status.as_u16().into())
        }
    }

    async fn del_multi<S>(&self, keys: &[S]) -> Result<()>
    where
        S: AsRef<str> + Sync,
    {
        for key in keys {
            self.del(key.as_ref()).await?;
        }
        Ok(())
    }

    // AWOS-JS 貌似省略了 content-type 和 content-md5。 这里也先省略
    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<SignedUrlOptions<'a>>>,
    {
        let opts = opts.into().unwrap_or_default();
        let expires = opts.expires.unwrap_or(3600);
        let method = opts.method.unwrap_or("GET");
        Ok(self.get_signed_url(key.as_ref(), method, expires, "", None))
    }
}
