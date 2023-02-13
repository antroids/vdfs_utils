use crc::{Algorithm, Crc};

const CRC_32_VDFS: Algorithm<u32> = Algorithm {
    width: 32,
    poly: 0x04c11db7,
    init: 0x00000000,
    refin: true,
    refout: true,
    xorout: 0x00000000,
    check: 0x2dfd2d88,
    residue: 0x00000000,
};

pub fn crc32(bytes: &[u8]) -> u32 {
    let crc = Crc::<u32>::new(&CRC_32_VDFS);
    crc.checksum(bytes)
}
