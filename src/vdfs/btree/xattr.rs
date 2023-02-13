use super::*;

#[derive(Debug)]
pub struct XattrTree<'a, S: DataSourceSource> {
    pub btree: VdfsBtree<'a, S>,
}

impl<'a, S: DataSourceSource> XattrTree<'a, S> {
    pub fn new(
        data_source: &'a DataSource<S>,
        super_blocks: Vdfs4SuperBlocks,
        base_table: DataPointer<Vdfs4BaseTable>,
    ) -> Result<XattrTree<'a, S>, BtreeError> {
        let btree = VdfsBtree::new(data_source, super_blocks, base_table, BtreeType::XAttrTree)?;
        Ok(XattrTree { btree })
    }
}
