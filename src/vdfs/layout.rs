use consts::*;

pub mod consts;

pub trait HasCrc32 {
    fn get_crc32(&self) -> u32;
    fn get_body_without_crc32<'a>(&'a self, bytes: &'a [u8]) -> &[u8] {
        let length_without_crc32 = bytes.len() - 4;
        &bytes[..length_without_crc32]
    }
}

pub trait HasSignature {
    fn get_signature(&self) -> &[u8];
    fn check_signature(&self, string: &str) -> bool {
        let signature = self.get_signature();
        if signature.len() == string.len() {
            for (i, c) in string.chars().into_iter().enumerate() {
                if !c.eq(&(signature[i] as char)) {
                    //println!("Signature mismatch: string: {} signature {} != char {} at index {}", string, signature[i] as char, string.as_bytes()[i] as char,i);
                    return false;
                }
            }
        }
        true
    }
}

pub trait VdfsBtreeKey: PartialEq + PartialOrd + bincode::Encode + bincode::Decode {
    fn get_generic_key(&self) -> &Vdfs4GenericKey;

    fn get_value_offset(&self) -> u64 {
        self.get_generic_key().key_len as u64
    }
}

pub trait VdfsBtreeNode: HasSignature + bincode::Encode + bincode::Decode + HasVersion {
    fn check_node_signature(&self) -> bool;
}

pub trait HasVersion {
    fn get_version(&self) -> u64;
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4SuperBlocks {
    sign1: Vdfs4SuperBlock,
    sign2: Vdfs4SuperBlock,
    pub super_block: Vdfs4SuperBlock,
    pub ext_super_block: Vdfs4ExtendedSuperBlock,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4SuperBlock {
    pub signature: [u8; 4], /* VDFS4 */
    pub layout_version: [u8; 4],
    pub maximum_blocks_count: u64,
    pub creation_timestamp: Vdfs4Timespec,
    pub volume_uuid: [u8; 16],
    pub volume_name: [u8; 16],
    pub mkfs_version: [u8; 64],
    pub unused: [u8; 40],
    pub log_block_size: u8,
    pub log_super_page_size: u8,
    pub log_erase_block_size: u8,
    pub case_insensitive: bool,
    pub read_only: bool,
    pub image_crc32_present: bool,
    pub force_full_decomp_decrypt: bool,
    pub hash_type: u8,
    pub encryption_flags: u8,
    pub sign_type: u8,
    pub reserved: [u8; 54],
    pub exsb_checksum: u32,
    pub basetable_checksum: u32,
    pub meta_hashtable_checksum: u32,
    pub image_inode_count: u64,
    pub pad: u32,
    pub sb_hash: [u8; VDFS4_MAX_CRYPTED_HASH_LEN],
    pub checksum: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4ExtendedSuperBlock {
    pub files_count: u64,
    pub folders_count: u64,
    pub volume_body: Vdfs4Extent,
    pub mount_counter: u32,
    pub sync_counter: u32, /* not used */
    pub umount_counter: u32,
    pub generation: u32,
    pub debug_area: Vdfs4Extent,
    pub meta_tbc: u32,
    pub pad: u32,
    pub tables: Vdfs4Extent,
    pub meta: [Vdfs4Extent; VDFS4_META_BTREE_EXTENTS],
    pub extension: Vdfs4Extent, /* not used */
    pub volume_blocks_count: u64,
    pub crc: u8,
    pub volume_uuid: [u8; 16],
    pub _reserved: [u8; 7],
    pub kbytes_written: u64,
    pub meta_hashtable_area: Vdfs4Extent,
    pub reserved: [u8; 860],
    pub checksum: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4Extent {
    pub begin: u64,
    pub length: u64,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4Timespec {
    pub seconds: u32,
    pub seconds_high: u32,
    pub nanoseconds: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4GeneralBtreeNode {
    pub magic: [u8; 4],
    pub version: [u32; 2],
    pub free_space: u16,
    pub recs_count: u16,
    pub node_id: u32,
    pub nrev_node_id: u32,
    pub next_node_id: u32,
    pub node_type: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4HeadBtreeNode {
    pub magic: [u8; 4],
    pub version: [u32; 2],
    pub root_bnode_id: u32,
    pub btree_height: u16,
    pub padding: [u8; 2],
}
//bitmap follows this structure

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4SnapshotDescriptor {
    pub signature: [u8; 4],
    pub sync_count: u32,
    pub mount_count: u64,
    pub checksum_offset: u64,
}

#[derive(bincode::Decode, bincode::Encode, Debug, Clone, Copy)]
pub struct Vdfs4BaseTable {
    pub descriptor: Vdfs4SnapshotDescriptor,
    pub last_page_index: [u64; VDFS4_SF_NR as usize],
    pub translation_table_offsets: [u64; VDFS4_SF_NR as usize],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4BaseTableRecord {
    pub meta_iblock: u64,
    pub sync_count: u32,
    pub mount_count: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4ExtendedRecord {
    pub object_id: u64,
    pub table_index: u64,
    pub meta_iblock: u64,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4ExtendedTable {
    pub descriptor: Vdfs4SnapshotDescriptor,
    pub records_count: u32,
    pub pad: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4MetaHashtable {
    pub signature: [u8; 4],
    pub pad: u32,
    pub size: u64,
    pub hashtable_offsets: [u64; VDFS4_SF_NR as usize],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4GenericKey {
    pub magic: [u8; 4],
    pub key_len: u16,
    pub record_len: u16,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CatTreeKey {
    pub gen_key: Vdfs4GenericKey,
    /** Object id of parent object (directory) */
    pub parent_id: u64,
    /** Object id of child object (file) */
    pub object_id: u64,
    /** Catalog tree record type */
    pub record_type: u8,
    /** Object's name */
    pub name_len: u8,
    pub name: [u8; VDFS4_FILE_NAME_LEN],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4XattrTreeKey {
    pub gen_key: Vdfs4GenericKey,
    pub object_id: u64,
    pub name_len: u8,
    pub name: [u8; VDFS4_XATTR_NAME_MAX_LEN],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CatalogFolderRecord {
    pub flags: u32,
    pub generation: u32,
    /** Amount of files in the directory */
    pub total_items_count: u64,
    /** Link's count for file */
    pub links_count: u64,
    /** Next inode in orphan list */
    pub next_orphan_id: u64,
    pub file_mode: u16,
    pub pad: u16,
    pub uid: u32,
    pub gid: u32,
    pub creation_time: Vdfs4Timespec,
    pub modification_time: Vdfs4Timespec,
    pub access_time: Vdfs4Timespec,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CatalogFileRecord {
    pub common: Vdfs4CatalogFolderRecord,
    pub data_fork: Vdfs4Fork,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4Fork {
    /** The size in bytes of the valid data in the fork */
    pub size_in_bytes: u64,
    /** The total number of allocation blocks which is
     * allocated for file system object under last actual
     * snapshot in this fork */
    pub total_blocks_count: u64,
    /** The set of extents which describe file system
     * object's blocks placement */
    pub extents: [Vdfs4Iextent; VDFS4_EXTENTS_COUNT_IN_FORK],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4Iextent {
    /** file data location */
    pub extent: Vdfs4Extent,
    /** extent start block logical index */
    pub iblock: u64,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CatalogHlinkRecord {
    /** file mode */
    pub file_mode: u16,
    pub pad1: u16,
    pub pad2: u16,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4ExtTreeKey {
    pub gen_key: Vdfs4GenericKey,
    pub object_id: u64,
    pub iblock: u64,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4ExtTreeRecord {
    pub key: Vdfs4ExtTreeKey,
    pub lextent: Vdfs4Extent,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct GenericIndexValue {
    pub node_id: u32,
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CompressedFileDescr {
    /* new fields are added before magic for easier backward compatibility */
    pub reserved: [u8; 7],
    pub sign_type: u8,
    /* from here same as layout 0x0005 */
    pub magic: [u8; 4],
    pub extents_num: u16,
    pub layout_version: u16,
    pub unpacked_size: u64,
    pub crc: u32,
    pub log_chunk_size: u32,
    pub aes_nonce: [u8; VDFS4_AES_NONCE_SIZE],
}

#[derive(bincode::Decode, bincode::Encode, Debug)]
pub struct Vdfs4CompressedExtent {
    pub magic: [u8; 2], // or profiled_prio
    pub flags: u16,
    pub len_bytes: u32,
    pub start: u64,
}

impl HasCrc32 for Vdfs4SuperBlock {
    fn get_crc32(&self) -> u32 {
        self.checksum
    }
}

impl HasSignature for Vdfs4SuperBlock {
    fn get_signature(&self) -> &[u8] {
        &self.signature
    }
}

impl HasCrc32 for Vdfs4ExtendedSuperBlock {
    fn get_crc32(&self) -> u32 {
        self.checksum
    }
}

impl HasSignature for Vdfs4GeneralBtreeNode {
    fn get_signature(&self) -> &[u8] {
        &self.magic
    }
}

impl VdfsBtreeNode for Vdfs4GeneralBtreeNode {
    fn check_node_signature(&self) -> bool {
        self.check_signature(VDFS4_BTREE_HEAD_NODE_MAGIC)
            || self.check_signature(VDFS4_BTREE_NODE_MAGIC)
    }
}

impl HasSignature for Vdfs4HeadBtreeNode {
    fn get_signature(&self) -> &[u8] {
        &self.magic
    }
}

impl VdfsBtreeNode for Vdfs4HeadBtreeNode {
    fn check_node_signature(&self) -> bool {
        self.check_signature(VDFS4_BTREE_HEAD_NODE_MAGIC)
    }
}

impl HasSignature for Vdfs4SnapshotDescriptor {
    fn get_signature(&self) -> &[u8] {
        &self.signature
    }
}

impl Vdfs4SnapshotDescriptor {
    pub fn get_version(&self) -> u64 {
        (self.mount_count << 32) | self.sync_count as u64
    }
}

impl HasSignature for Vdfs4MetaHashtable {
    fn get_signature(&self) -> &[u8] {
        &self.signature
    }
}

impl HasSignature for Vdfs4CompressedFileDescr {
    fn get_signature(&self) -> &[u8] {
        &self.magic
    }
}

impl HasCrc32 for Vdfs4CompressedFileDescr {
    fn get_crc32(&self) -> u32 {
        self.crc
    }
}

impl Vdfs4CompressedFileDescr {
    pub fn get_compression(&self) -> Option<VdfsFileCompression> {
        if self.check_signature(VDFS4_COMPR_ZIP_FILE_DESCR_MAGIC) {
            return Some(VdfsFileCompression::Zlib);
        } else if self.check_signature(VDFS4_COMPR_GZIP_FILE_DESCR_MAGIC) {
            return Some(VdfsFileCompression::Gzip);
        } else if self.check_signature(VDFS4_COMPR_LZO_FILE_DESCR_MAGIC) {
            return Some(VdfsFileCompression::Lzo);
        }
        None
    }

    pub fn get_auth(&self) -> Option<VdfsFileAuth> {
        if self.magic[0] as char == VDFS4_MD5_AUTH {
            return Some(VdfsFileAuth::Md5);
        } else if self.magic[0] as char == VDFS4_SHA1_AUTH {
            return Some(VdfsFileAuth::Sha1);
        } else if self.magic[0] as char == VDFS4_SHA256_AUTH {
            return Some(VdfsFileAuth::Sha256);
        }
        None
    }

    pub fn get_signature_type(&self) -> Option<VdfsFileSignatureType> {
        VdfsFileSignatureType::from_u8(self.sign_type)
    }
}

impl HasSignature for Vdfs4CompressedExtent {
    fn get_signature(&self) -> &[u8] {
        &self.magic
    }
}

impl Vdfs4CompressedExtent {
    pub fn check_extent_signature(&self) -> bool {
        self.check_signature(VDFS4_COMPR_EXT_MAGIC)
    }

    pub fn has_uncompressed_flag(&self) -> bool {
        self.flags & VDFS4_CHUNK_FLAG_UNCOMPR != 0
    }

    pub fn has_encrypted_flag(&self) -> bool {
        self.flags & VDFS4_CHUNK_FLAG_ENCRYPTED != 0
    }
}

impl Vdfs4BaseTable {
    pub fn get_translated_position(
        &self,
        base_table_offset: u64,
        base_table: &Vdfs4BaseTable,
        table_type: impl TranslationTableIndex,
    ) -> u64 {
        base_table_offset + base_table.translation_table_offsets[table_type.get_index()]
    }
}

impl PartialEq for Vdfs4GenericKey {
    fn eq(&self, other: &Self) -> bool {
        self.magic == other.magic
            && self.key_len == other.key_len
            && self.record_len == other.record_len
    }
}

impl VdfsBtreeKey for Vdfs4CatTreeKey {
    fn get_generic_key(&self) -> &Vdfs4GenericKey {
        &self.gen_key
    }
}

impl PartialEq for Vdfs4CatTreeKey {
    fn eq(&self, other: &Self) -> bool {
        self.gen_key == other.gen_key
            && self.parent_id == other.parent_id
            && self.object_id == other.object_id
            && self.record_type == other.record_type
            && self.name_len == other.name_len
            && self.name[..(self.name_len as usize)] == other.name[..(self.name_len as usize)]
    }
}

impl PartialOrd for Vdfs4CatTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.parent_id.partial_cmp(&other.parent_id) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.name[..(self.name_len as usize)]
            .partial_cmp(&other.name[..(other.name_len as usize)])
        {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.name_len.partial_cmp(&other.name_len) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.object_id.partial_cmp(&other.object_id) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        Some(core::cmp::Ordering::Equal)
    }
}

impl VdfsBtreeKey for Vdfs4ExtTreeKey {
    fn get_generic_key(&self) -> &Vdfs4GenericKey {
        &self.gen_key
    }
}

impl PartialEq for Vdfs4ExtTreeKey {
    fn eq(&self, other: &Self) -> bool {
        self.object_id == other.object_id && self.iblock == other.iblock
    }
}

impl PartialOrd for Vdfs4ExtTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.object_id.partial_cmp(&other.object_id) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.iblock.partial_cmp(&other.iblock)
    }
}

impl Vdfs4GenericKey {
    pub fn new() -> Self {
        Self {
            magic: [0u8; 4],
            key_len: 0,
            record_len: 0,
        }
    }
}

impl Vdfs4CatTreeKey {
    pub fn get_name_string(&self) -> String {
        let name_bytes = self.name[0..self.name_len as usize].to_vec();
        String::from_utf8(name_bytes).unwrap()
    }

    pub fn get_record_type(&self) -> CatalogTreeRecordType {
        CatalogTreeRecordType::from_u8(self.record_type).unwrap()
    }

    pub fn child_of_root() -> Self {
        Self {
            gen_key: Vdfs4GenericKey::new(),
            parent_id: SpecialInodeIds::Root as u64,
            object_id: 0,
            record_type: 0,
            name_len: 0,
            name: [0u8; VDFS4_FILE_NAME_LEN],
        }
    }
}

impl Vdfs4ExtTreeKey {
    pub fn from_object_id(object_id: u64) -> Self {
        Self {
            gen_key: Vdfs4GenericKey::new(),
            object_id,
            iblock: 0,
        }
    }
}

impl HasVersion for Vdfs4BaseTableRecord {
    fn get_version(&self) -> u64 {
        ((self.mount_count as u64) << 32) + self.sync_count as u64
    }
}

impl HasVersion for Vdfs4GeneralBtreeNode {
    fn get_version(&self) -> u64 {
        ((self.version[1] as u64) << 32) + self.version[0] as u64
    }
}

impl Vdfs4GeneralBtreeNode {
    pub fn get_last_record_index(&self) -> u16 {
        self.recs_count - 1
    }
}

impl HasVersion for Vdfs4HeadBtreeNode {
    fn get_version(&self) -> u64 {
        ((self.version[1] as u64) << 32) + self.version[0] as u64
    }
}

impl Vdfs4CatalogFolderRecord {
    pub fn is_file_type(&self, file_type: FileType) -> bool {
        file_type.is_file_type(self.file_mode)
    }

    pub fn get_file_type(&self) -> Option<FileType> {
        FileType::from_u16(self.file_mode)
    }

    pub fn has_file_flag(&self, flag: VdfsFileFlags) -> bool {
        if let VdfsFileFlags::HardLink = flag {
            if self.links_count > 1 {
                return true;
            }
        }
        self.flags & (1 << (flag as u32)) != 0
    }
}
