use crate::vdfs::data_source::*;
use super::*;

#[derive(Debug)]
pub struct ExtentTree<'a, S: DataSourceSource> {
    pub btree: VdfsBtree<'a, S>
}

impl<'a, S: DataSourceSource> ExtentTree<'a, S> {
    pub fn new(data_source: &'a DataSource<S>, 
                super_blocks: Vdfs4SuperBlocks, 
                base_table: DataPointer<Vdfs4BaseTable>) -> Result<ExtentTree<'a, S>, BtreeError> {
        let btree = VdfsBtree::new(data_source, super_blocks, base_table, BtreeType::ExtentsTree)?;
        Ok(ExtentTree {
            btree
        })
    }

    pub fn records_iterator(&self, first_object_id: u64) -> Result<BtreeRecordsIterator<S, Vdfs4ExtTreeKey>, BtreeError> {
        let key = Vdfs4ExtTreeKey::from_object_id(first_object_id);
        let root_child_node = self.btree.find(&key)?;
        self.btree.records_iter(root_child_node.bnode.data.node_id, root_child_node.record_index)
    }
}