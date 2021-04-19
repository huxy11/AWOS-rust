use std::{
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    str::FromStr,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Region {
    /// 华东-杭州
    HangZhou,
    /// 华东-上海
    ShangHai,
    /// 华北-青岛
    QingDao,
    /// 华北-北京
    BeiJing,
    /// 华北-张家口
    ZhangJiaKou,
    /// 华北-呼和浩特
    HuHeHaoTe,
    /// 华北-乌兰察布
    WuLanChaBu,
    /// 华南-深圳
    ShenZhen,
    /// 华南-河源
    HeYuan,
    /// 华南-广州
    GuangZhou,
    /// 西南-成都
    ChengDu,
    /// Region that covers China
    HongKong,
    /// Region that covers the Western part of the United States
    UsWest,
    /// Region that covers the Eastern part of the United States
    UsEast,
    /// Region that covers the South-Eastern part of Asia Pacific
    ApSoutheast1,
    /// Region that covers the South-Eastern part of Asia Pacific
    ApSoutheast2,
    /// Region that covers the South-Eastern part of Asia Pacific
    ApSoutheast3,
    /// Region that covers the South-Eastern part of Asia Pacific
    ApSoutheast5,
    /// Region that covers the North-Eastern part of Asia Pacific
    ApNortheast,
    /// Region that covers the Southern part of Asia Pacific
    ApSouth,
    /// Region that covers Central Europe
    EuCentral,
    /// Region that covers Western Europe
    EuWest,
    /// Region that covers Eastern Europe
    EuEast,
    /// Region that covers the Eastern part of Middle East
    MeEast,
    //TODO below:
    // Custom {
    //     name: String,
    //     endpoint: String,
    // },
}

impl Region {
    pub fn name(&self) -> &'static str {
        match *self {
            Self::BeiJing => "cn-beijin",
            _ => unimplemented!(),
        }
    }
    pub fn endpoint(&self) -> &'static str {
        match *self {
            Self::BeiJing => "oss-cn-beijing.aliyuncs.com",
            _ => unimplemented!(),
        }
    }
}

impl Default for Region {
    fn default() -> Region {
        std::env::var("OSS_DEFAULT_REGION")
            .or_else(|_| std::env::var("OSS_REGION"))
            .ok()
            .and_then(|ref s| Region::from_str(s).ok())
            .unwrap_or(Region::BeiJing)
    }
}
#[derive(Debug, PartialEq)]
pub struct ParseRegionError {
    message: String,
}
impl ParseRegionError {
    /// Parses a region given as a string literal into a type `Region'
    pub fn new(input: &str) -> Self {
        ParseRegionError {
            message: format!(
                "Either an unimplemented or an invalid OSS region: {}, ",
                input
            ),
        }
    }
}
impl StdError for ParseRegionError {}
impl Display for ParseRegionError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{}", self.message)
    }
}

impl FromStr for Region {
    type Err = ParseRegionError;

    fn from_str(s: &str) -> Result<Self, ParseRegionError> {
        let v: &str = &s.to_lowercase();
        match v {
            "北京" | "oss-cn-beijing" | "osscnbeijing" => Ok(Region::BeiJing),
            s => Err(ParseRegionError::new(s)),
        }
    }
}
