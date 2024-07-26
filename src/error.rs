use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("GenericError: {message}, {location}"))]
    Generic {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("InvalidDataError: {message}, {location}"))]
    InvalidData {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("IoError: {message}, {location}"))]
    Io {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("ArrowError: {message}, {location}"))]
    Arrow {
        message: String,
        location: snafu::Location,
    },
}

trait ToSnafuLocation {
    fn to_snafu_location(&'static self) -> snafu::Location;
}

impl ToSnafuLocation for std::panic::Location<'static> {
    fn to_snafu_location(&'static self) -> snafu::Location {
        snafu::Location::new(self.file(), self.line(), self.column())
    }
}

macro_rules! make_error_from {
    ($from:ty, $to:ident) => {
        impl From<$from> for Error {
            fn from(value: $from) -> Self {
                Self::$to {
                    message: value.to_string(),
                    location: std::panic::Location::caller().to_snafu_location(),
                }
            }
        }
    };
}

make_error_from!(std::io::Error, Io);
make_error_from!(arrow::error::ArrowError, Arrow);
