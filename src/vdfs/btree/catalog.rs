use crate::vdfs::data_source::*;
use super::*;


#[derive(Debug)]
pub struct CatalogTree<'a, S: DataSourceSource> {
    pub btree: VdfsBtree<'a, S>
}

impl<'a, 'b: 'a, S: DataSourceSource> CatalogTree<'a, S> {
    pub fn new(data_source: &'b DataSource<S>, 
                super_blocks: Vdfs4SuperBlocks, 
                base_table: DataPointer<Vdfs4BaseTable>) -> Result<CatalogTree<'a, S>, BtreeError> {
        let btree = VdfsBtree::new(data_source, super_blocks, base_table, BtreeType::CatalogTree)?;
        Ok(CatalogTree {
            btree
        })
    }

    pub fn all_records_iterator(&self) -> Result<BtreeRecordsIterator<S, Vdfs4CatTreeKey>, BtreeError> {
        let key = Vdfs4CatTreeKey::child_of_root();
        let root_child_node = self.btree.find(&key)?;
        self.btree.records_iter(root_child_node.bnode.data.node_id, root_child_node.record_index)
    }
}