use bincode::Encode;
use std::{mem::size_of, collections::BTreeMap, fs::{self, File}, io::Write};

use self::{layout::*, layout::consts::*, data_source::*, btree::catalog::CatalogTree, btree::{extent::ExtentTree, BtreeError}, btree::xattr::XattrTree};

pub mod layout;
pub mod btree;
pub mod vdfs_crc;
pub mod data_source;

pub mod unpack;

#[derive(Debug)]
pub enum VdfsError {
    BtreeError(BtreeError),
    DataSourceError(DataSourceError),
    BaseTableIsMissing(),
    FileWriteError(String),
    FileBlockNotFound(u64),
    DecompressionError,
    CompressedFileExtentWrongSignature,
    CannotDecompressFileWithoutCompression,
    CannotFindParentFolder,
}

pub struct Vdfs<'a, S: DataSourceSource> {
    data_source: &'a DataSource<S>,
    block_size: u64,

    super_blocks: Vdfs4SuperBlocks,

    bitmap_size_in_bytes: u64,

    current_base_table: Option<DataPointer<Vdfs4BaseTable>>,

    catalog_btree: Option<CatalogTree<'a, S>>,
    extent_btree: Option<ExtentTree<'a, S>>,
    xattr_btree: Option<XattrTree<'a, S>>,
}

impl<'a, S: DataSourceSource> Vdfs<'a, S> {
    pub fn new(data_source: &'a DataSource<S>) -> Result<Vdfs<'a, S>, BtreeError> {
        let block_size: u64 = consts::BLOCK_SIZE_DEFAULT;
        let super_page_size = consts::SUPER_PAGE_SIZE_DEFAULT;

        let super_blocks_offset = 0;
        let super_blocks: Vdfs4SuperBlocks = data_source.read_at(super_blocks_offset)?.data;

        let bitmap_size_in_bytes = super_page_size - size_of::<Vdfs4HeadBtreeNode>() as u64 - size_of::<u32>() as u64;

        Ok(Self { 
            data_source,
            block_size,
            super_blocks,
            bitmap_size_in_bytes,

            current_base_table: None,

            catalog_btree: None,
            extent_btree: None,
            xattr_btree: None,
        })
    }

}

impl<'a, S: DataSourceSource> Vdfs<'a, S> {
    
    pub fn get_super_blocks(&self) -> &Vdfs4SuperBlocks {
        &self.super_blocks
    }

    pub fn read_btree_head(&self, extent_offset: u64) -> Result<DataPointer<Vdfs4HeadBtreeNode>, VdfsError> {
        self.data_source.read_at(self.blocks_to_bytes(extent_offset)).map_err(|e| VdfsError::DataSourceError(e))
    }

    pub fn read_bitmap_bit(&self, extent_offset: u64, bit_offset: u64) -> Result<bool, BtreeError> {
        let bitmap_offset_in_bytes = self.blocks_to_bytes(extent_offset) + size_of::<Vdfs4HeadBtreeNode>() as u64;
        let byte_offset_in_bitmap = bit_offset / 8;
        let bit_offset_in_byte = bit_offset % 8;
        let offset_in_bytes = bitmap_offset_in_bytes + byte_offset_in_bitmap;

        assert!(byte_offset_in_bitmap < self.bitmap_size_in_bytes);

        let byte: u8 = self.data_source.read_at(offset_in_bytes)?.data;
        Ok(byte & (1 << bit_offset_in_byte) != 0)
    }

    pub fn get_bitmap_size_in_bits(&self) -> u64 {
        self.bitmap_size_in_bytes * 8
    }

    fn get_base_table_offset(&self, index: u32) -> u64 {
        assert!(index < 2);
        let tables_extent_begin = self.super_blocks.ext_super_block.tables.begin;
        let tables_extent_length = self.super_blocks.ext_super_block.tables.length;
        let tables_length_in_bytes = self.blocks_to_bytes(tables_extent_length);
        let first_table_offset_in_bytes = self.blocks_to_bytes(tables_extent_begin);
        first_table_offset_in_bytes + index as u64 * (tables_length_in_bytes / 2)
    }

    pub fn init_current_base_table(&mut self) -> Result<(), VdfsError> {
        let base_tables = self.read_base_tables()?;
        let first = base_tables.0.ok_or(VdfsError::BaseTableIsMissing())?;

        if let Some(second) = base_tables.1 {
            if first.data.descriptor.get_version() < second.data.descriptor.get_version() {
                self.current_base_table = Some(second);
                return Ok(());
            }
        }
        self.current_base_table = Some(first);
        Ok(())
    }

    pub fn read_base_tables(&self) -> Result<(Option<DataPointer<Vdfs4BaseTable>>, Option<DataPointer<Vdfs4BaseTable>>), VdfsError> {
        let first_table_offset_in_bytes = self.get_base_table_offset(0);
        let second_table_offset_in_bytes = self.get_base_table_offset(1);

        let first_table = self.read_base_table(first_table_offset_in_bytes)?;
        let second_table = self.read_base_table(second_table_offset_in_bytes)?;

        Ok((first_table, second_table))
    }

    fn read_base_table(&self, offset_in_bytes: u64) -> Result<Option<DataPointer<Vdfs4BaseTable>>, VdfsError> {
        let base_table: DataPointer<Vdfs4BaseTable> = self.data_source.read_at(offset_in_bytes)?;
        let size_without_crc32 = base_table.data.descriptor.checksum_offset;
        if !base_table.data.descriptor.check_signature(VDFS4_SNAPSHOT_BASE_TABLE) || base_table.data.descriptor.checksum_offset > size_without_crc32 { 
            return Ok(Option::None); 
        }
        if self.check_crc32_of_snapshot_descriptor(base_table.position, &base_table.data.descriptor)? {
            return Ok(Option::Some(base_table));
        }
        Ok(Option::None)
    }

    fn check_crc32_of_snapshot_descriptor(&self, offset_in_bytes: u64, snapshot_descriptor: &Vdfs4SnapshotDescriptor) -> Result<bool, VdfsError> {
        let crc32_from_data: u32 = self.data_source.read_at(offset_in_bytes + snapshot_descriptor.checksum_offset)?.data;
        let body = self.data_source.read_bytes_at(offset_in_bytes, snapshot_descriptor.checksum_offset)?;
        let crc32_calculated = vdfs_crc::crc32(body.as_slice());
        Ok(crc32_from_data == crc32_calculated)
    }

    pub fn read_extended_tables(&self, base_table: &DataPointer<Vdfs4BaseTable>) {
        let base_table_offset = base_table.position;
        let base_table_size = base_table.data.descriptor.checksum_offset as usize + CRC32_SIZE;
        let mut extended_table_offset = base_table_offset + size_ceil_to_block(base_table_size, VDFS4_SNAPSHOT_EXT_SIZE) as u64;

        for extended_table_index in 0..VDFS4_SNAPSHOT_EXT_TABLES {
            let extended_table: Vdfs4ExtendedTable = self.data_source.read_at(extended_table_offset).unwrap().data;

            println!("Base table at {}, Extended Table at {}: {:?}", base_table_offset, extended_table_offset, extended_table);

            if !extended_table.descriptor.check_signature(VDFS4_SNAPSHOT_EXTENDED_TABLE) 
                || !(self.check_crc32_of_snapshot_descriptor(extended_table_offset, &extended_table.descriptor).unwrap()) {
                break;
            }
        }
        todo!("Extent tables not implemented yet");
    }

    fn blocks_to_bytes(&self, blocks: u64) -> u64 {
        self.block_size * blocks
    }

    fn calc_crc32(&self, data: &(impl Encode + HasCrc32)) -> Result<u32, VdfsError> {
        let encoded = self.data_source.serialize(data)?;
        let slice_without_crc32 = data.get_body_without_crc32(encoded.as_slice());
        Ok(vdfs_crc::crc32(&slice_without_crc32))
    }

    fn validate_crc32(&self, data: &(impl Encode + HasCrc32)) -> Result<bool, VdfsError> {
        let crc32_from_data = data.get_crc32();
        let crc32_calculated = self.calc_crc32(data)?;
        Ok(crc32_from_data == crc32_calculated)
    }

    pub fn init_btrees(&mut self) -> Result<(), VdfsError> {
        let base_table = self.current_base_table.ok_or(VdfsError::BaseTableIsMissing())?;
        let data_source = self.data_source;
        let btree = CatalogTree::new(data_source, self.super_blocks, base_table)?;
        self.catalog_btree = Some(btree);
        let btree = ExtentTree::new(&self.data_source, self.super_blocks, base_table)?;
        self.extent_btree = Some(btree);
        let btree = XattrTree::new(&self.data_source, self.super_blocks, base_table)?;
        self.xattr_btree = Some(btree);
        Ok(())
    }
}

impl From<DataSourceError> for VdfsError {
    fn from(value: DataSourceError) -> Self {
        VdfsError::DataSourceError(value)
    }
}

impl From<BtreeError> for VdfsError {
    fn from(value: BtreeError) -> Self {
        VdfsError::BtreeError(value)
    }
}