use std::mem::size_of;

use super::{Vdfs4CatTreeKey, Vdfs4XattrTreeKey, Vdfs4ExtTreeKey, Vdfs4CatalogFileRecord, Vdfs4ExtTreeRecord};

const fn max_u64(a: u64, b: u64) -> u64 {
    if a > b {
		return a;
	} else {
		return b;
	}
}

pub const CRC32_SIZE: usize = 4;

pub const VDFS4_FILE_NAME_LEN: usize = 255;
pub const VDFS4_FULL_PATH_LEN: usize = 1023;

pub const VDFS4_XATTR_NAME_MAX_LEN: usize = 200;
pub const VDFS4_XATTR_VAL_MAX_LEN: usize = 200;

pub const VDFS4_CAT_KEY_MAX_LEN: u64 = size_ceil_to_block(size_of::<Vdfs4CatTreeKey>(), 8) as u64;
pub const VDFS4_XATTR_KEY_MAX_LEN: u64 = size_ceil_to_block(size_of::<Vdfs4XattrTreeKey>(), 8) as u64;
pub const VDFS4_EXT_KEY_MAX_LEN: u64 = size_ceil_to_block(size_of::<Vdfs4ExtTreeKey>(), 8) as u64;

pub const VDFS4_KEY_MAX_LEN: u64 = max_u64(VDFS4_CAT_KEY_MAX_LEN, max_u64(VDFS4_XATTR_KEY_MAX_LEN, VDFS4_EXT_KEY_MAX_LEN));

pub const VDFS4_EXTENTS_COUNT_IN_FORK: usize = 9;

pub const VDFS4_SNAPSHOT_BASE_TABLE: &str = "CoWB";
pub const VDFS4_SNAPSHOT_EXTENDED_TABLE: &str = "CoWE";

pub const VDFS4_BTREE_HEAD_NODE_MAGIC: &str = "eHND";
pub const VDFS4_BTREE_NODE_MAGIC: &str = "Nd";

pub const VDFS4_COMPR_ZIP_FILE_DESCR_MAGIC: &str = "CZip";
pub const VDFS4_COMPR_GZIP_FILE_DESCR_MAGIC: &str = "CGzp";
pub const VDFS4_COMPR_LZO_FILE_DESCR_MAGIC: &str = "CLzo";
pub const VDFS4_MD5_AUTH: char = 'I';
pub const VDFS4_SHA1_AUTH: char = 'H';
pub const VDFS4_SHA256_AUTH: char = 'h';
pub const VDFS4_COMPR_EXT_MAGIC: &str = "XT";

pub const VDFS4_CHUNK_FLAG_UNCOMPR: u16 = 0x1;
pub const VDFS4_CHUNK_FLAG_ENCRYPTED: u16 = 0x2;

pub const VDFS4_MD5_HASH_LEN: usize = 16;
pub const VDFS4_SHA1_HASH_LEN: usize = 20;
pub const VDFS4_SHA256_HASH_LEN: usize = 32;
// const VDFS4_RSA1024_SIGN_LEN: usize = 128;
// const VDFS4_RSA2048_SIGN_LEN: usize = 256;
pub const VDFS4_MAX_CRYPTED_HASH_LEN: usize = 256;
// const VDFS4_HW_COMPR_PAGE_PER_CHUNK: usize = 32;
// const VDFS4_MIN_LOG_CHUNK_SIZE: usize = 12;
// const VDFS4_MAX_LOG_CHUNK_SIZE: usize = 20;
pub const VDFS4_AES_NONCE_SIZE: usize = 8;
// const VDFS4_AES_KEY_LENGTH: usize = 16;
// const VDFS4_AES_CHUNK_ALIGN_LEN: usize = 16;
// const VDFS4_AES_CHUNK_ALIGN_START: usize = 16;

pub const VDFS4_SNAPSHOT_EXT_SIZE: usize = 4096;
pub const VDFS4_SNAPSHOT_EXT_TABLES: usize = 8;
// const VDFS4_SNAPSHOT_BASE_TABLES: usize = 2;

pub const VDFS4_SF_NR: u32 = VDFS4_LSFILE - VDFS4_FSFILE + 1;

pub const VDFS4_META_BTREE_EXTENTS: usize = 96;

pub const VDFS4_INVALID_NODE_ID: u64 = 0;

pub enum SpecialInodeIds {
	RootDirObject = 0,	/** parent_id of root inode */
	Root = 1,		/** root inode */
	CatTree = 2,		/** catalog tree inode */
	SpaceBitmap = 3,		/** free space bitmap inode */
	ExtentsTree = 4,		/** inode bitamp inode number */
	FreeInodeBitmap = 5,	/** Free space bitmap inode */
	XattrTree = 6,		/** XAttr tree ino */
	Snapshot = 7,
	OrphanInodes = 8,		/** FIXME remove this line breaks fsck*/
	FirstFile = 9		// First file inode
}

pub const VDFS4_FSFILE: u32 = SpecialInodeIds::CatTree as u32;
pub const VDFS4_LSFILE: u32 = SpecialInodeIds::XattrTree as u32;

#[derive(Debug, Clone, Copy)]
pub enum BnodeType {
	CatalogTree = 2,
	SpaceBitmap = 3,
	ExtentsTree = 4,
	FreeInodeBitmap = 5,
	XAttrTree = 6
}

#[derive(Debug, Clone, Copy)]
pub enum BtreeType {
	CatalogTree = BnodeType::CatalogTree as isize,
	ExtentsTree = BnodeType::ExtentsTree as isize,
	XAttrTree = BnodeType::XAttrTree as isize
}

#[derive(Debug, Clone, Copy)]
pub enum CatalogTreeRecordType {
	Dummy = 0,
	Folder = 1,
	File = 2,
	HLink = 3,
	ILink = 5,
	UnpackInode = 10,
}

pub enum VdfsFileFlags {
	HasBlocksInExttree = 1,
	Immutable = 2,
	HardLink = 10,
	OrphanInode = 12,
	CompressedFile = 13,
	AuthFile = 15,
	ReadOnlyAuth = 16,
	EncryptedFile = 17,
	ProfiledFile = 18,
}

pub enum VdfsFileCompression {
	Zlib,
	Gzip,
	Lzo
}

pub enum VdfsFileAuth {
	Md5,
	Sha1,
	Sha256,
}

pub enum VdfsFileSignatureType {
	None = 0x0,
	Rsa1024 = 0x1,
	Rsa2048 = 0x2,
}


pub trait TranslationTableIndex {
	fn get_index(self) -> usize;
}

impl TranslationTableIndex for BnodeType {
    fn get_index(self) -> usize {
        self as usize - 2
    }
}

impl TranslationTableIndex for BtreeType {
    fn get_index(self) -> usize {
        self as usize - 2
    }
}

impl BtreeType {
	pub fn get_max_record_len(self) -> u64 {
		match self {
			BtreeType::CatalogTree => VDFS4_CAT_KEY_MAX_LEN + size_of::<Vdfs4CatalogFileRecord>() as u64,
			BtreeType::ExtentsTree => VDFS4_EXT_KEY_MAX_LEN + size_of::<Vdfs4ExtTreeRecord>() as u64,
			BtreeType::XAttrTree => VDFS4_XATTR_KEY_MAX_LEN + VDFS4_XATTR_VAL_MAX_LEN as u64,
		}
	}
}

impl SpecialInodeIds {
	pub fn from_u32(index: u32) -> Option<SpecialInodeIds> {
		match index {
			0 => return Some(SpecialInodeIds::RootDirObject),
			1 => return Some(SpecialInodeIds::Root),
			2 => return Some(SpecialInodeIds::CatTree),
			3 => return Some(SpecialInodeIds::SpaceBitmap),
			4 => return Some(SpecialInodeIds::ExtentsTree),
			5 => return Some(SpecialInodeIds::FreeInodeBitmap),
			6 => return Some(SpecialInodeIds::XattrTree),
			7 => return Some(SpecialInodeIds::Snapshot),
			8 => return Some(SpecialInodeIds::OrphanInodes),
			9 => return Some(SpecialInodeIds::FirstFile),
			_ => return None
		}
	}
}

impl CatalogTreeRecordType {
	pub fn from_u8(index: u8) -> Option<CatalogTreeRecordType> {
		match index {
			0 => return Some(Self::Dummy),
			1 => return Some(Self::Folder),
			2 => return Some(Self::File),
			3 => return Some(Self::HLink),
			5 => return Some(Self::ILink),
			10 => return Some(Self::UnpackInode),
			_ => return None,
		}
	}
}

pub const BLOCK_SIZE_DEFAULT: u64 = 4096;
pub const SUPER_PAGE_SIZE_DEFAULT: u64 = 16384;

pub const fn size_ceil_to_block(size: usize, block_size: usize) -> usize {
    ((size + block_size - 1) / block_size) * block_size
}

impl VdfsFileSignatureType {
	pub fn get_signature_length(&self) -> u64 {
		match self {
			VdfsFileSignatureType::None => 0,
			VdfsFileSignatureType::Rsa1024 => 128,
			VdfsFileSignatureType::Rsa2048 => 256,
		}
	}

	pub fn from_u8(value: u8) -> Option<Self> {
        match value {
			0 => Some(Self::None),
			1 => Some(Self::Rsa1024),
			2 => Some(Self::Rsa2048),
			_ => None
		}
    }
}

impl VdfsFileAuth {
	pub fn get_hash_len(&self) -> u64 {
		match self {
			VdfsFileAuth::Md5 => VDFS4_MD5_HASH_LEN as u64,
			VdfsFileAuth::Sha1 => VDFS4_SHA1_HASH_LEN as u64,
			VdfsFileAuth::Sha256 => VDFS4_SHA256_HASH_LEN as u64,
		}
	}
}



// GNU

/* Encoding of the file mode.  */

pub const FILE_TYPE_MASK: u16 = 0o0170000;	/* These bits determine file type.  */

#[derive(Debug, Clone, Copy)]
pub enum FileType {
	Directory = 0o0040000,
	CharacterDevice = 0o0020000,
	BlockDevice = 0o0060000,
	Regular = 0o0100000,
	Fifo = 0o0010000,
	SymbolicLink = 0o0120000,
	Socket = 0o0140000,
}

impl FileType {
	pub fn to_u16(&self) -> u16 {
		*self as u16
	}

	pub fn from_u16(file_type: u16) -> Option<FileType> {
		if FileType::Directory.is_file_type(file_type) { Some(FileType::Directory) }
			else if FileType::CharacterDevice.is_file_type(file_type) { Some(FileType::CharacterDevice) }
			else if FileType::BlockDevice.is_file_type(file_type) { Some(FileType::BlockDevice) }
			else if FileType::Regular.is_file_type(file_type) { Some(FileType::Regular) }
			else if FileType::Fifo.is_file_type(file_type) { Some(FileType::Fifo) }
			else if FileType::SymbolicLink.is_file_type(file_type) { Some(FileType::SymbolicLink) }
			else if FileType::Socket.is_file_type(file_type) { Some(FileType::Socket) }
			else { None }
	}

	pub fn is_file_type(&self, file_mode: u16) -> bool {
		(file_mode & FILE_TYPE_MASK) == self.to_u16()
	}
}