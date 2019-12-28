use std::io::{self, Write, BufRead, BufReader, BufWriter, Cursor};
use std::mem;
use std::path::PathBuf;
use std::str::{self, FromStr};

use byteorder::{BigEndian, ReadBytesExt};
use chrono::NaiveDateTime;
use heed::byteorder::BE;
use heed::{EnvOpenOptions, Error, LmdbError};
use heed::types::*;
use heed::zerocopy::U64;
use main_error::MainError;
use structopt::StructOpt;

const ONE_BILLION: u64 = 1_000_000_000;
const DATETIME_FORMAT: &str = "%FT%T%.f";

type BEU64 = U64<BE>;
type SmallVec8<T> = smallvec::SmallVec<[T; 8]>;

// The character codes are:
//   * `f` - a 32 bit float (f32)
//   * `F` - a 64 bit float (f64)
//   * `u` - a 32 bit unsigned integer (u32)
//   * `U` - a 64 bit unsigned integer (u64)
//   * `i` - a 32 bit signed integer (i32)
//   * `I` - a 64 bit signed integer (i64)
#[derive(Debug, Clone, Copy)]
enum Code {
    Float,
    Double,
    Unsigned,
    UnsignedLong,
    Signed,
    SignedLong,
}

impl Code {
    fn from(c: u8) -> Option<Code> {
        match c {
            b'f' => Some(Code::Float),
            b'F' => Some(Code::Double),
            b'u' => Some(Code::Unsigned),
            b'U' => Some(Code::UnsignedLong),
            b'i' => Some(Code::Signed),
            b'I' => Some(Code::SignedLong),
            _ => None,
        }
    }

    fn size(self) -> usize {
        match self {
            Code::Float => mem::size_of::<f32>(),
            Code::Double => mem::size_of::<f64>(),
            Code::Unsigned => mem::size_of::<u32>(),
            Code::UnsignedLong => mem::size_of::<u64>(),
            Code::Signed => mem::size_of::<i32>(),
            Code::SignedLong => mem::size_of::<i64>(),
        }
    }
}

#[derive(StructOpt)]
#[structopt(about = "the stupid content tracker")]
enum Opt {
    Write(WriteOpt),
    Read(ReadOpt),
    Infos(InfosOpt),
}

#[derive(StructOpt)]
struct WriteOpt {
    #[structopt(short, long, parse(from_os_str))]
    database: PathBuf,
}

#[derive(StructOpt)]
struct ReadOpt {
    #[structopt(short, long, parse(from_os_str))]
    database: PathBuf,
    #[structopt(long)]
    filter: Option<glob::Pattern>,
}

#[derive(StructOpt)]
struct InfosOpt {
    #[structopt(short, long, parse(from_os_str))]
    database: PathBuf,
}

fn write_to_database(opt: WriteOpt) -> Result<(), MainError> {
    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(opt.database)?;

    // let dbinfos = env.create_database::<Str, Str>(Some("infos"))?;
    let db = env.create_database::<OwnedType<BEU64>, ByteSlice>(None)?;
    let mut wtxn = env.write_txn()?;

    let mut values_code = db.get(&wtxn, &BEU64::new(0))?.map(ToOwned::to_owned);

    let mut buffer = Vec::new();
    let reader = BufReader::new(io::stdin());

    for result in reader.lines() {
        let line = result?;
        buffer.clear();

        let mut iter = line.split_whitespace();
        let text = iter.next().ok_or("missing text")?;
        let date = iter.next().ok_or("missing date")?;
        let code = iter.next().ok_or("missing code")?;
        let values = iter.clone();

        let code = match values_code {
            Some(ref old_code) if &old_code[..] == code.as_bytes() => code,
            Some(_) => return Err("invalid code".into()),
            None => {
                db.put(&mut wtxn, &BEU64::new(0), code.as_bytes())?;
                values_code = Some(code.as_bytes().to_owned());
                code
            },
        };

        if code.len() != iter.count() {
            return Err("wrong number of values".into());
        }

        let dt = NaiveDateTime::parse_from_str(date, DATETIME_FORMAT)?;
        let nanos = BEU64::new(dt.timestamp_nanos() as u64);

        for (c, n) in code.as_bytes().iter().zip(values) {
            match Code::from(*c) {
                Some(Code::Float) => {
                    let bytes = f32::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                Some(Code::Double) => {
                    let bytes = f64::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                Some(Code::Unsigned) => {
                    let bytes = u32::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                Some(Code::UnsignedLong) => {
                    let bytes = u64::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                Some(Code::Signed) => {
                    let bytes = i32::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                Some(Code::SignedLong) => {
                    let bytes = i64::from_str(n)?;
                    buffer.extend_from_slice(&bytes.to_be_bytes());
                },
                None => return Err("Invalid code character".into()),
            }
        }

        buffer.extend_from_slice(text.as_bytes());

        match db.append(&mut wtxn, &nanos, &buffer) {
            Ok(()) => (),
            Err(Error::Lmdb(LmdbError::KeyExist)) => {
                return Err("inserted value not ordered".into())
            },
            Err(error) => return Err(error.into()),
        }
    }

    wtxn.commit()?;

    Ok(())
}

fn read_from_database(opt: ReadOpt) -> Result<(), MainError> {
    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(opt.database)?;

    let db = match env.open_database::<OwnedType<BEU64>, ByteSlice>(None)? {
        Some(db) => db,
        None => return Err("database not found".into()),
    };

    let rtxn = env.read_txn()?;
    let mut iter = db.iter(&rtxn)?;

    let code = match iter.next() {
        Some(result) => {
            let (_, code) = result?;
            code
        },
        None => return Ok(()),
    };

    let mut value_size = 0;
    let mut codes = SmallVec8::new();

    for c in code {
        let code = Code::from(*c).unwrap();
        value_size += code.size();
        codes.push(code);
    }

    let mut writer = BufWriter::new(io::stdout());

    for result in iter {
        let (nanos, bytes) = result?;

        let dt = {
            let secs = nanos.get() / ONE_BILLION;
            let nsecs = nanos.get() % ONE_BILLION;
            let dt = NaiveDateTime::from_timestamp(secs as i64, nsecs as u32);

            dt.format(DATETIME_FORMAT)
        };

        let text = str::from_utf8(&bytes[value_size..])?;

        if let Some(pattern) = opt.filter.as_ref() {
            if !pattern.matches(text) {
                continue
            }
        }

        write!(&mut writer, "{} {} ", text, dt)?;

        let mut cursor = Cursor::new(bytes);
        for (i, code) in codes.iter().enumerate() {
            match code {
                Code::Float => {
                    let value = cursor.read_f32::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
                Code::Double => {
                    let value = cursor.read_f64::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
                Code::Unsigned => {
                    let value = cursor.read_u32::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
                Code::UnsignedLong => {
                    let value = cursor.read_u64::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
                Code::Signed => {
                    let value = cursor.read_i32::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
                Code::SignedLong => {
                    let value = cursor.read_i64::<BigEndian>()?;
                    write!(&mut writer, "{}", value)?;
                },
            }

            if i != codes.len() - 1 {
                write!(&mut writer, " ")?;
            }
        }

        writeln!(&mut writer)?;
    }

    Ok(())
}

fn infos_of_database(opt: InfosOpt) -> Result<(), MainError> {
    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(opt.database)?;

    let db = match env.open_database::<OwnedType<BEU64>, ByteSlice>(None)? {
        Some(db) => db,
        None => return Err("database not found".into()),
    };

    let rtxn = env.read_txn()?;
    let code = db.get(&rtxn, &BEU64::new(0))?;
    if let Some(code) = code {
        let code = str::from_utf8(code)?;
        println!("values code: {}", code);
    }

    let len = db.len(&rtxn)?;
    let len = len.saturating_sub(1);
    println!("number of entries: {}", len);

    Ok(())
}

// oceanic-airlines 2001-01-13T12:09:14.026490 ff 37.686751 -122.602227
fn main() -> Result<(), MainError> {
    match Opt::from_args() {
        Opt::Write(opt) => write_to_database(opt),
        Opt::Read(opt) => read_from_database(opt),
        Opt::Infos(opt) => infos_of_database(opt),
    }
}
