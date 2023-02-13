use std::env;
use std::fs::{self, File};

use vdfs::data_source::DataSourceSource;

use crate::vdfs::{data_source, Vdfs};

mod vdfs;

impl DataSourceSource for File {}

fn main() {
    let input_path = env::args().nth(1).expect("Input file not specified");
    let output_path = env::args().nth(2).expect("Output file not specified");

    println!(
        "Unpacking vdfs filesystem from {} to folder: {}",
        &input_path, &output_path
    );

    let vdfs_file = File::open(&input_path).expect("Cannot open file");
    let data_source = data_source::DataSource::from_source(vdfs_file);

    fs::remove_dir_all(&output_path).ok();

    let mut vdfs = Vdfs::new(&data_source).expect("Cannot initialize Vdfs");

    vdfs.init_current_base_table()
        .expect("Cannot initialize base table");
    vdfs.init_btrees().expect("Cannot initialize Btrees");
    vdfs.unpack(&output_path).unwrap();
}
