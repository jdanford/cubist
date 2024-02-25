use std::{fmt, time::{Duration, SystemTime, UNIX_EPOCH}};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        SystemTime::now().into()
    }
}

impl Into<SystemTime> for Timestamp {
    fn into(self) -> SystemTime {
        let seconds = self.0;
        UNIX_EPOCH + Duration::from_secs(seconds)
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).expect("system time is out of range");
        let seconds = duration.as_secs();
        Timestamp(seconds)
    }
}

// impl fmt::Display for Timestamp {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         let time: SystemTime = (*self).into();
//         fmt::Display::fmt(time, f)
//     }
// }
