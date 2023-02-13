use libflate::{gzip, zlib};
use std::io::Read;

use super::*;

impl<'a, S: DataSourceSource> Vdfs<'a, S> {
    pub fn unpack(&self, output_path: &str) -> Result<(), VdfsError> {
        let root_path = String::from(output_path);
        let mut folders_map = BTreeMap::<u64, String>::new();

        fs::create_dir_all(&root_path).unwrap();
        for record in self
            .catalog_btree
            .as_ref()
            .unwrap()
            .all_records_iterator()
            .unwrap()
        {
            if let CatalogTreeRecordType::Folder = record.data.get_record_type() {
                let folder_record: Vdfs4CatalogFolderRecord =
                    record.get_record_value(self.data_source)?.data;
                let mut path = String::new();
                let parent_folder_id = record.data.parent_id;

                if folder_record.next_orphan_id != VDFS4_INVALID_NODE_ID {
                    todo!();
                }
                if parent_folder_id == VDFS4_INVALID_NODE_ID {
                    path.push_str(&root_path);
                } else {
                    let parent_folder = folders_map.get(&parent_folder_id).unwrap();
                    path.push_str(parent_folder);
                }
                add_path_component(&mut path, record.data.get_name_string().as_str());
                fs::create_dir(&path).unwrap();
                folders_map.insert(record.data.object_id, path);
            }
        }

        for record in self
            .catalog_btree
            .as_ref()
            .unwrap()
            .all_records_iterator()
            .unwrap()
        {
            if record.data.parent_id == record.data.object_id {
                println!(
                    "Record object_id == record_parent_id, scipping: {:?}",
                    record
                );
                continue;
            }

            match record.data.get_record_type() {
                CatalogTreeRecordType::File => {
                    let catalog_file_record: Vdfs4CatalogFileRecord =
                        record.get_record_value(self.data_source)?.data;
                    let parent_id = record.data.parent_id;
                    let mut path = String::new();
                    //println!("FILE: {:?} {:?}", record.data.get_name_string(), catalog_file_record);

                    if catalog_file_record.common.is_file_type(FileType::Regular) {
                        if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::AuthFile)
                        {
                            println!("AuthFile");
                            continue;
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::CompressedFile)
                        {
                            println!("CompressedFile");
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::EncryptedFile)
                        {
                            println!("EncryptedFile");
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::HardLink)
                        {
                            println!("HardLink");
                            continue;
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::HasBlocksInExttree)
                        {
                            println!("HasBlocksInExttree");
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::Immutable)
                        {
                            println!("Immutable");
                            continue;
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::OrphanInode)
                        {
                            println!("OrphanInode");
                            continue;
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::ProfiledFile)
                        {
                            println!("ProfiledFile");
                            continue;
                        } else if catalog_file_record
                            .common
                            .has_file_flag(VdfsFileFlags::ReadOnlyAuth)
                        {
                            println!("ReadOnlyAuth");
                            continue;
                        }

                        if parent_id != VDFS4_INVALID_NODE_ID {
                            let parent_folder = folders_map
                                .get(&parent_id)
                                .ok_or(VdfsError::CannotFindParentFolder)?;
                            add_path_component(&mut path, &parent_folder);
                        } else {
                            path.push_str(&root_path);
                        }
                        add_path_component(&mut path, &record.data.get_name_string());

                        println!("Regular file: {}", path);

                        self.unpack_file(&path, record.data.object_id, &catalog_file_record)?;
                    } else {
                        println!(
                            "Scipping special file: {:?}",
                            catalog_file_record.common.get_file_type().unwrap()
                        );
                    }
                }
                CatalogTreeRecordType::HLink => {
                    let catalog_hlink_record: Vdfs4CatalogHlinkRecord =
                        record.get_record_value(self.data_source)?.data;
                    println!(
                        "HLINK: {:?} {:?}",
                        record.data.get_name_string(),
                        catalog_hlink_record
                    );
                }
                CatalogTreeRecordType::UnpackInode => todo!(),
                _ => {}
            }
        }

        Ok(())
    }

    fn unpack_file(
        &self,
        path: &str,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
    ) -> Result<(), VdfsError> {
        if catalog_file_record
            .common
            .has_file_flag(VdfsFileFlags::CompressedFile)
        {
            self.unpack_compressed_file(path, file_object_id, catalog_file_record)
        } else if catalog_file_record
            .common
            .has_file_flag(VdfsFileFlags::EncryptedFile)
        {
            self.unpack_compressed_file(path, file_object_id, catalog_file_record)
        } else {
            self.unpack_raw_file(path, file_object_id, catalog_file_record)
        }
    }

    fn unpack_compressed_file(
        &self,
        path: &str,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
    ) -> Result<(), VdfsError> {
        let temp_raw_file =
            self.create_temp_file_and_write_raw_data(path, file_object_id, catalog_file_record)?;
        let temp_file_data_source = DataSource::from_source(temp_raw_file);
        let descriptor_size = size_of::<Vdfs4CompressedFileDescr>() as u64;
        let extent_size = size_of::<Vdfs4CompressedExtent>() as u64;
        let temp_raw_file_size = catalog_file_record.data_fork.size_in_bytes;

        let descriptor: Vdfs4CompressedFileDescr = temp_file_data_source
            .read_at(temp_raw_file_size - descriptor_size as u64)?
            .data;
        let compressed_flag = catalog_file_record
            .common
            .has_file_flag(VdfsFileFlags::CompressedFile);
        let compression = descriptor.get_compression();
        let signature_type = descriptor.get_signature_type();
        let auth_type = descriptor.get_auth();
        let extents_count = descriptor.extents_num as u64;

        if auth_type.is_some() {
            println!("SKIPPING FILE WITH AUTH: {}", path);
            return Ok(());
            //todo!("Implement auth")
        }
        if !compressed_flag || compression.is_none() {
            return Err(VdfsError::CannotDecompressFileWithoutCompression);
        }

        let mut first_extent_position =
            temp_raw_file_size - descriptor_size as u64 - extent_size * extents_count;
        if signature_type.is_some() {
            first_extent_position -= signature_type.unwrap().get_signature_length();
        }
        if auth_type.is_some() {
            first_extent_position -= auth_type.unwrap().get_hash_len() * (extents_count + 1);
        }

        let mut output_file = File::create(path)
            .map_err(|e| VdfsError::FileWriteError(format!("Cannot create file: {}", e)))?;
        for extent_index in 0..descriptor.extents_num {
            let extent_position = first_extent_position + extent_size * extent_index as u64;
            let extent: Vdfs4CompressedExtent =
                temp_file_data_source.read_at(extent_position)?.data;

            println!("Extent: {:?}", extent);

            if !extent.check_extent_signature() {
                return Err(VdfsError::CompressedFileExtentWrongSignature);
            }
            if compressed_flag && extent.has_encrypted_flag() {
                todo!("Encryption");
            }

            let mut chunk_buffer =
                temp_file_data_source.read_bytes_at(extent.start, extent.len_bytes as u64)?;
            if extent.has_uncompressed_flag() {
                output_file
                    .write_all(chunk_buffer.as_mut_slice())
                    .map_err(|e| {
                        VdfsError::FileWriteError(format!("Cannot append to file: {}", e))
                    })?;
            } else {
                match compression.as_ref().unwrap() {
                    VdfsFileCompression::Zlib => {
                        let mut decoder = zlib::Decoder::new(chunk_buffer.as_slice()).unwrap();
                        let mut decoded_buffer = Vec::<u8>::new();
                        decoder
                            .read_to_end(&mut decoded_buffer)
                            .map_err(|_| VdfsError::DecompressionError)?;
                        output_file
                            .write_all(decoded_buffer.as_mut_slice())
                            .map_err(|e| {
                                VdfsError::FileWriteError(format!("Cannot append to file: {}", e))
                            })?;
                    }
                    VdfsFileCompression::Gzip => {
                        let mut decoder = gzip::Decoder::new(chunk_buffer.as_slice()).unwrap();
                        let mut decoded_buffer = Vec::<u8>::new();
                        decoder
                            .read_to_end(&mut decoded_buffer)
                            .map_err(|_| VdfsError::DecompressionError)?;
                        output_file
                            .write_all(decoded_buffer.as_mut_slice())
                            .map_err(|e| {
                                VdfsError::FileWriteError(format!("Cannot append to file: {}", e))
                            })?;
                    }
                    VdfsFileCompression::Lzo => todo!(),
                }
            }
        }

        Ok(())
    }

    fn unpack_raw_file(
        &self,
        path: &str,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
    ) -> Result<(), VdfsError> {
        let mut file = fs::File::create(&path).unwrap();
        self.write_raw_data_to_file(&mut file, path, file_object_id, catalog_file_record)?;
        Ok(())
    }

    fn create_temp_file_and_write_raw_data(
        &self,
        path: &str,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
    ) -> Result<File, VdfsError> {
        let mut temp_raw_file =
            tempfile::tempfile().map_err(|e| VdfsError::FileWriteError(e.to_string()))?;
        self.write_raw_data_to_file(
            &mut temp_raw_file,
            path,
            file_object_id,
            catalog_file_record,
        )?;
        Ok(temp_raw_file)
    }

    fn write_raw_data_to_file(
        &self,
        file: &mut File,
        path: &str,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
    ) -> Result<(), VdfsError> {
        let mut bytes_left = catalog_file_record.data_fork.size_in_bytes;
        let mut iblock = 0; // file logical block index

        while bytes_left > 0 {
            let iblock_position_in_blocks =
                self.get_file_iblock_position(file_object_id, catalog_file_record, iblock)?;
            let iblock_position_in_bytes = self.blocks_to_bytes(iblock_position_in_blocks);
            let bytes_left_in_iblock = self.block_size.min(bytes_left);
            let readed = self
                .data_source
                .read_bytes_at(iblock_position_in_bytes, bytes_left_in_iblock)?;
            file.write_all(readed.as_slice()).map_err(|_| {
                VdfsError::FileWriteError(format!("Cannot write to file: {}", path))
            })?;

            bytes_left -= bytes_left_in_iblock;
            iblock += 1;
        }

        Ok(())
    }

    fn get_file_iblock_position(
        &self,
        file_object_id: u64,
        catalog_file_record: &Vdfs4CatalogFileRecord,
        iblock: u64,
    ) -> Result<u64, VdfsError> {
        if iblock > catalog_file_record.data_fork.total_blocks_count {
            panic!("File logical block is out of range");
        }
        for extent in &catalog_file_record.data_fork.extents {
            if extent.iblock + extent.extent.length > iblock {
                // this iblock is in the extent
                let start_iblock_to_read = extent.extent.begin + iblock - extent.iblock;
                return Ok(start_iblock_to_read);
            }
        }

        let extents_tree = self.extent_btree.as_ref().unwrap();
        let extent_tree_iterator = extents_tree.records_iterator(file_object_id)?;

        for extent_tree_record_key in extent_tree_iterator {
            if extent_tree_record_key.data.object_id == file_object_id {
                let extent_tree_record_value: Vdfs4ExtTreeRecord = extent_tree_record_key
                    .get_record_value(self.data_source)?
                    .data;
                let last_iblock_in_record =
                    extent_tree_record_value.key.iblock + extent_tree_record_value.lextent.length;
                if last_iblock_in_record > iblock {
                    let start_iblock_to_read = extent_tree_record_value.lextent.begin + iblock
                        - extent_tree_record_value.key.iblock;
                    return Ok(start_iblock_to_read);
                }
            } else {
                break;
            }
        }

        Err(VdfsError::FileBlockNotFound(iblock))
    }
}

fn add_path_component(path: &mut String, component: &str) {
    path.push('/');
    path.push_str(component);
}
