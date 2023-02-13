use std::mem::size_of;

use bincode::Decode;

use super::{data_source::*, layout::consts::*, layout::*};

pub mod catalog;
pub mod extent;
pub mod xattr;

#[derive(Debug)]
pub enum BtreeError {
    DataSourceError(DataSourceError),
    NodeAndBaseTableVersionsMismatch(u64, u64),
    InvalidNodeSignature(),
    BnodeRecordIndexOutOfBounds(u16),
    BnodeRecordOffsetOutOfBounds(u32),
    BnodeRecordValueOffsetOutOfBounds(u64),
    LevelIsTooHigh(u16, u16),
    LeftRecordKeyIsHigherThanSearchKey(),
}

#[derive(Debug)]
pub struct Bnode {
    pub descriptor: Vdfs4GeneralBtreeNode,
}

#[derive(Debug, Clone, Copy)]
pub struct BnodeRecordInfo<T: VdfsBtreeKey> {
    pub key: DataPointer<T>,
    pub bnode: DataPointer<Vdfs4GeneralBtreeNode>,
    pub record_index: u16,
}

const VDFS4_BTREE_LEAF_LVL: u16 = 1;

#[derive(Debug)]
pub struct VdfsBtree<'a, S: DataSourceSource> {
    data_source: &'a DataSource<S>,

    super_blocks: Vdfs4SuperBlocks,
    base_table: DataPointer<Vdfs4BaseTable>,

    node_size_bytes: u64,
    block_size: u64,
    btree_type: BtreeType,

    head_node: Option<DataPointer<Vdfs4HeadBtreeNode>>,
}

pub struct BtreeRecordsIterator<'a, S: DataSourceSource, T: VdfsBtreeKey> {
    btree: &'a VdfsBtree<'a, S>,
    bnode_record_info: BnodeRecordInfo<T>,
    initial_state: bool,
}

impl<'a, 'b: 'a, S: DataSourceSource> VdfsBtree<'a, S> {
    pub fn new(
        data_source: &'b DataSource<S>,
        super_blocks: Vdfs4SuperBlocks,
        base_table: DataPointer<Vdfs4BaseTable>,
        btree_type: BtreeType,
    ) -> Result<VdfsBtree<'a, S>, BtreeError> {
        let log_blocks_in_leb =
            super_blocks.super_block.log_super_page_size - super_blocks.super_block.log_block_size;
        let block_size: u64 = 1 << super_blocks.super_block.log_block_size;
        let node_size_bytes = (1 << log_blocks_in_leb) * block_size;

        let mut btree = VdfsBtree {
            data_source,
            super_blocks,
            base_table,
            node_size_bytes,
            block_size,
            btree_type,

            head_node: None,
        };

        let head_bnode: DataPointer<Vdfs4HeadBtreeNode> =
            Self::read_base_table_record(&btree, data_source, &base_table, 0)?;

        btree.head_node = Some(head_bnode);
        Ok(btree)
    }

    fn get_iblock_offset(&self, metablock: u64) -> u64 {
        let mut total_area_size = 0u64;
        for i in 0..VDFS4_META_BTREE_EXTENTS {
            let length = self.super_blocks.ext_super_block.meta[i].length;
            let offset = self.super_blocks.ext_super_block.meta[i].begin;
            total_area_size += length;
            if total_area_size > metablock {
                return (offset + length) - (total_area_size - metablock);
            }
        }
        0u64
    }

    fn read_base_table_record<T: VdfsBtreeNode>(
        &self,
        data_source: &DataSource<impl DataSourceSource>,
        base_table: &DataPointer<Vdfs4BaseTable>,
        node_id: u32,
    ) -> Result<DataPointer<T>, BtreeError> {
        let first_record_position = base_table.data.get_translated_position(
            base_table.position,
            &base_table.data,
            self.btree_type,
        );
        let record_size = size_of::<Vdfs4BaseTableRecord>();
        let record_position = first_record_position + record_size as u64 * node_id as u64;
        let table_record: Vdfs4BaseTableRecord = data_source.read_at(record_position).unwrap().data;
        let iblock_position = self.get_iblock_offset(table_record.meta_iblock) * self.block_size;
        let iblock_descriptor: DataPointer<T> = data_source.read_at(iblock_position)?;

        if table_record.get_version() != iblock_descriptor.data.get_version() {
            return Err(BtreeError::NodeAndBaseTableVersionsMismatch(
                iblock_descriptor.data.get_version(),
                table_record.get_version(),
            ));
        }

        if !iblock_descriptor.data.check_node_signature() {
            return Err(BtreeError::InvalidNodeSignature());
        }

        Ok(iblock_descriptor)
    }

    fn get_bnode<T: VdfsBtreeNode>(&self, node_id: u32) -> Result<DataPointer<T>, BtreeError> {
        self.read_base_table_record(self.data_source, &self.base_table, node_id)
    }

    fn get_bnode_offset_offset(&self, index: u16) -> u64 {
        self.node_size_bytes - CRC32_SIZE as u64 - size_of::<u32>() as u64 * (index + 1) as u64
    }

    fn get_bnode_offset_position(
        &self,
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
        index: u16,
    ) -> u64 {
        bnode.position + self.get_bnode_offset_offset(index)
    }

    fn get_bnode_offset_from_data_source(
        &self,
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
        index: u16,
    ) -> Result<u32, BtreeError> {
        if index > bnode.data.get_last_record_index() {
            return Err(BtreeError::BnodeRecordIndexOutOfBounds(index));
        }

        let offset_position = self.get_bnode_offset_position(bnode, index);
        let bnode_offset: u32 = self.data_source.read_at(offset_position)?.data;

        if bnode_offset == 0 || bnode_offset as u64 >= self.node_size_bytes {
            return Err(BtreeError::BnodeRecordOffsetOutOfBounds(bnode_offset));
        }
        Ok(bnode_offset)
    }

    fn get_bnode_offset_from_buffer(
        &self,
        buffer: &[u8],
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
        index: u16,
    ) -> Result<u32, BtreeError> {
        if index > bnode.data.get_last_record_index() {
            return Err(BtreeError::BnodeRecordIndexOutOfBounds(index));
        }

        let bnode_offset_offset = self.get_bnode_offset_offset(index);
        let bnode_offset: u32 = self
            .data_source
            .deserialize(&buffer[bnode_offset_offset as usize..])?;
        if bnode_offset == 0 || bnode_offset as u64 >= self.node_size_bytes {
            return Err(BtreeError::BnodeRecordOffsetOutOfBounds(bnode_offset));
        }

        Ok(bnode_offset)
    }

    fn get_bnode_record<T: VdfsBtreeKey>(
        &self,
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
        index: u16,
    ) -> Result<DataPointer<T>, BtreeError> {
        let bnode_offset: u32 = self.get_bnode_offset_from_data_source(bnode, index)?;
        let bnode_record: DataPointer<T> = self
            .data_source
            .read_at(bnode.position + bnode_offset as u64)?;
        Ok(bnode_record)
    }

    fn get_bnode_record_from_buffer<T: VdfsBtreeKey>(
        &self,
        buffer: &[u8],
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
        index: u16,
    ) -> Result<DataPointer<T>, BtreeError> {
        let bnode_offset: u32 = self.get_bnode_offset_from_buffer(buffer, bnode, index)?;
        let bnode_record: T = self
            .data_source
            .deserialize(&buffer[bnode_offset as usize..])?;
        Ok(DataPointer {
            data: bnode_record,
            position: bnode.position + bnode_offset as u64,
        })
    }

    fn traverse<T: VdfsBtreeKey>(
        &self,
        key: &T,
        till_level: u16,
    ) -> Result<BnodeRecordInfo<T>, BtreeError> {
        let head_bnode = self.head_node.as_ref().unwrap();
        let max_tree_level = head_bnode.data.btree_height;

        if till_level >= max_tree_level {
            return Err(BtreeError::LevelIsTooHigh(till_level, max_tree_level));
        }

        let mut bnode_id = head_bnode.data.root_bnode_id;
        let mut bnode_record_info = self.traverse_level(key, bnode_id)?;

        for level in ((till_level + 1)..=max_tree_level).rev() {
            let index_value: GenericIndexValue = bnode_record_info
                .key
                .get_record_value(self.data_source)?
                .data;
            bnode_id = index_value.node_id;
            bnode_record_info = self.traverse_level(key, bnode_id)?;
        }

        Ok(bnode_record_info)
    }

    fn traverse_level<T: VdfsBtreeKey>(
        &self,
        key: &T,
        start_bnode_id: u32,
    ) -> Result<BnodeRecordInfo<T>, BtreeError> {
        let mut bnode: DataPointer<Vdfs4GeneralBtreeNode> =
            self.read_base_table_record(self.data_source, &self.base_table, start_bnode_id)?;
        let mut bnode_buffer = self
            .data_source
            .read_bytes_at(bnode.position, self.node_size_bytes)?;
        let (mut bnode_record_index, mut bnode_record): (u16, DataPointer<T>) =
            self.binary_search_in_bnode(&bnode_buffer, key, &bnode)?;

        // Dangling nodes are not handled
        while bnode_record_index == bnode.data.get_last_record_index()
            && bnode.data.next_node_id != 0
        {
            bnode = self.read_base_table_record(
                self.data_source,
                &self.base_table,
                bnode.data.next_node_id,
            )?;
            bnode_buffer = self
                .data_source
                .read_bytes_at(bnode.position, self.node_size_bytes)?;
            (bnode_record_index, bnode_record) =
                self.binary_search_in_bnode(&bnode_buffer, key, &bnode)?;
        }

        Ok(BnodeRecordInfo {
            key: bnode_record,
            bnode: bnode,
            record_index: bnode_record_index,
        })
    }

    fn binary_search_in_bnode<T: VdfsBtreeKey>(
        &self,
        bnode_buffer: &[u8],
        key: &T,
        bnode: &DataPointer<Vdfs4GeneralBtreeNode>,
    ) -> Result<(u16, DataPointer<T>), BtreeError> {
        let mut left_index: u16 = 0;
        let mut right_index = bnode.data.recs_count - 1;

        let mut left_record: DataPointer<T> =
            self.get_bnode_record_from_buffer(&bnode_buffer, bnode, left_index)?;
        if left_index == right_index || left_record.data == *key {
            return Ok((left_index, left_record));
        } else if left_record.data > *key {
            return Err(BtreeError::LeftRecordKeyIsHigherThanSearchKey());
        }

        let mut record = self.get_bnode_record_from_buffer(&bnode_buffer, bnode, right_index)?;
        if record.data == *key {
            return Ok((right_index, record));
        }

        while left_index < right_index - 1 {
            let middle_index = left_index + (right_index - left_index + 1) / 2;
            record = self.get_bnode_record_from_buffer(&bnode_buffer, bnode, middle_index)?;

            match record.data.partial_cmp(key).unwrap() {
                std::cmp::Ordering::Less => {
                    left_index = middle_index;
                    left_record = record;
                }
                std::cmp::Ordering::Equal => return Ok((middle_index, record)),
                std::cmp::Ordering::Greater => right_index = middle_index,
            }
        }

        return Ok((left_index, left_record));
    }

    pub fn find<T: VdfsBtreeKey>(&self, key: &T) -> Result<BnodeRecordInfo<T>, BtreeError> {
        self.traverse(key, VDFS4_BTREE_LEAF_LVL)
    }

    pub fn records_iter<T: VdfsBtreeKey>(
        &self,
        start_bnode_id: u32,
        start_index: u16,
    ) -> Result<BtreeRecordsIterator<S, T>, BtreeError> {
        let bnode: DataPointer<Vdfs4GeneralBtreeNode> = self.get_bnode(start_bnode_id).unwrap();
        let first_record: DataPointer<T> = self.get_bnode_record(&bnode, start_index).unwrap();

        Ok(BtreeRecordsIterator {
            btree: self,
            bnode_record_info: BnodeRecordInfo {
                key: first_record,
                bnode: bnode,
                record_index: start_index,
            },
            initial_state: true,
        })
    }
}

impl<'a, S: DataSourceSource, T: VdfsBtreeKey> Iterator for BtreeRecordsIterator<'a, S, T> {
    type Item = DataPointer<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let last_record_index = self.bnode_record_info.bnode.data.get_last_record_index();

        if !(self.initial_state && self.bnode_record_info.record_index <= last_record_index) {
            match self.bnode_record_info.record_index.cmp(&last_record_index) {
                std::cmp::Ordering::Less => self.bnode_record_info.record_index += 1,
                std::cmp::Ordering::Equal => {
                    let next_node_id = self.bnode_record_info.bnode.data.next_node_id;
                    if next_node_id != VDFS4_INVALID_NODE_ID as u32 {
                        let next_bnode: DataPointer<Vdfs4GeneralBtreeNode> =
                            self.btree.get_bnode(next_node_id).unwrap();
                        self.bnode_record_info.bnode = next_bnode;
                        self.bnode_record_info.record_index = 0;
                    } else {
                        return None;
                    }
                }
                std::cmp::Ordering::Greater => panic!("Tree was modified during iterating"),
            }
        }
        self.initial_state = false;
        return Some(
            self.btree
                .get_bnode_record(
                    &self.bnode_record_info.bnode,
                    self.bnode_record_info.record_index,
                )
                .unwrap(),
        );
    }
}

impl<T: VdfsBtreeKey> DataPointer<T> {
    pub fn get_record_value<R: Decode + Sized, S: DataSourceSource>(
        &self,
        data_source: &DataSource<S>,
    ) -> Result<DataPointer<R>, BtreeError> {
        let offset = self.data.get_value_offset();

        if offset > VDFS4_KEY_MAX_LEN {
            return Err(BtreeError::BnodeRecordValueOffsetOutOfBounds(offset));
        }

        let position = self.position + self.data.get_value_offset();
        data_source
            .read_at(position)
            .map_err(|e| BtreeError::DataSourceError(e))
    }
}

impl From<DataSourceError> for BtreeError {
    fn from(value: DataSourceError) -> Self {
        BtreeError::DataSourceError(value)
    }
}
