use std::{
    cell::RefCell,
    io::{self, Read, Seek, Write},
};

use bincode::{
    config::{Configuration, Fixint, LittleEndian, NoLimit, SkipFixedArrayLength},
    Decode, Encode,
};

pub trait DataSourceSource: Read + Write + Seek {}

#[derive(Debug)]
pub struct DataSource<S: DataSourceSource> {
    source: RefCell<S>,
}

#[derive(Debug, Clone, Copy)]
pub struct DataPointer<T> {
    pub data: T,
    pub position: u64,
}

#[derive(Debug, Decode, Encode)]
pub enum DataSourceError {
    WriteDataError,
    ReadDataError,
    SerializeError,
    DeserializeError,
}

pub trait ReadBytes {
    fn read_bytes_at(&self, position: u64, size: u64) -> Result<Vec<u8>, DataSourceError>;
}

pub trait WriteBytes {
    fn write_bytes_at(&self, data: &[u8], position: u64) -> Result<u64, DataSourceError>;
}

pub trait WriteData<W> {
    fn serialize(&self, data: &W) -> Result<Vec<u8>, DataSourceError>;
    fn write_at(&self, data: &W, position: u64) -> Result<u64, DataSourceError>;
    fn write(&self, data_pointer: DataPointer<W>) -> Result<u64, DataSourceError>;
}

pub trait ReadData<R> {
    fn deserialize(&self, data: &[u8]) -> Result<R, DataSourceError>;
    fn read_at(&self, position: u64) -> Result<DataPointer<R>, DataSourceError>;
}

const BINCODE_CONFIG: Configuration<LittleEndian, Fixint, SkipFixedArrayLength, NoLimit> =
    bincode::config::legacy()
        .skip_fixed_array_length()
        .with_fixed_int_encoding()
        .with_little_endian();

impl<'a, W: Encode + Sized, S: DataSourceSource> WriteData<W> for DataSource<S> {
    fn serialize(&self, data: &W) -> Result<Vec<u8>, DataSourceError> {
        bincode::encode_to_vec(data, BINCODE_CONFIG).map_err(|_| DataSourceError::SerializeError)
    }

    fn write_at(&self, data: &W, position: u64) -> Result<u64, DataSourceError> {
        let serialized = self
            .serialize(data)
            .map_err(|_| DataSourceError::WriteDataError)?;
        self.write_bytes_at(serialized.as_slice(), position)
    }

    fn write(&self, data_pointer: DataPointer<W>) -> Result<u64, DataSourceError> {
        self.write_at(&data_pointer.data, data_pointer.position)
    }
}

impl<'a, R: Decode + Sized, S: DataSourceSource> ReadData<R> for DataSource<S> {
    fn deserialize(&self, data: &[u8]) -> Result<R, DataSourceError> {
        bincode::decode_from_slice(&data, BINCODE_CONFIG)
            .map_err(|_| DataSourceError::DeserializeError)
            .map(|(result, _)| result)
    }

    fn read_at(&self, position: u64) -> Result<DataPointer<R>, DataSourceError> {
        //println!("DEBUG: {}", position);
        self.source
            .borrow_mut()
            .seek(io::SeekFrom::Start(position))
            .map_err(|_| DataSourceError::ReadDataError)?;
        bincode::decode_from_std_read(&mut *self.source.borrow_mut(), BINCODE_CONFIG)
            .map_err(|_| DataSourceError::ReadDataError)
            .map(|r| DataPointer { data: r, position })
    }
}

impl<'a, S: DataSourceSource> ReadBytes for DataSource<S> {
    fn read_bytes_at(&self, position: u64, size: u64) -> Result<Vec<u8>, DataSourceError> {
        self.source
            .borrow_mut()
            .seek(io::SeekFrom::Start(position))
            .map_err(|_| DataSourceError::ReadDataError)?;
        let mut buf = vec![0u8; size as usize];
        self.source
            .borrow_mut()
            .read_exact(buf.as_mut_slice())
            .map_err(|_| DataSourceError::ReadDataError)
            .map(|_| buf)
    }
}

impl<'a, S: DataSourceSource> WriteBytes for DataSource<S> {
    fn write_bytes_at(&self, data: &[u8], position: u64) -> Result<u64, DataSourceError> {
        self.source
            .borrow_mut()
            .seek(io::SeekFrom::Start(position))
            .map_err(|_| DataSourceError::WriteDataError)?;
        self.source
            .borrow_mut()
            .write_all(data)
            .map_err(|_| DataSourceError::WriteDataError)
            .map(|_| data.len() as u64)
    }
}

impl<S: DataSourceSource> DataSource<S> {
    pub fn from_source(source: S) -> DataSource<S> {
        DataSource {
            source: RefCell::new(source),
        }
    }
}
