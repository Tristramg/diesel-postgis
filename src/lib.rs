extern crate diesel;
extern crate byteorder;
extern crate geo;

use byteorder::{ByteOrder, LittleEndian, BigEndian};
use std::error::Error;
use std::io::prelude::*;
use diesel::pg::{Pg, PgTypeMetadata};
use diesel::row;
use diesel::types::{FromSql, ToSql, IsNull, HasSqlType, FromSqlRow};
use diesel::query_builder::QueryId;
use std::fmt;
pub use geo::*;


#[derive(Debug)]
struct NotImplemented {}
impl fmt::Display for NotImplemented {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Not implemented Error")
    }
}

impl Error for NotImplemented {
    fn description(&self) -> &str {
        "Not implemented"
    }
}


fn read_u32(bytes: &[u8], big_endian: bool) -> u32 {
    if big_endian {
        BigEndian::read_u32(bytes)
    } else {
        LittleEndian::read_u32(bytes)
    }
}

fn read_f64(bytes: &[u8], big_endian: bool) -> f64 {
    if big_endian {
        BigEndian::read_f64(bytes)
    } else {
        LittleEndian::read_f64(bytes)
    }
}

pub struct PgGeometry {}
pub type Geom = Geometry<f64>;

impl FromSql<PgGeometry, Pg> for Geom {
    fn from_sql(bytes: Option<&[u8]>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = bytes.unwrap();
        let big_endian = bytes[0] == 0u8;
        let type_id = read_u32(&bytes[1..5], big_endian);

        // let srid = if type_id & 0x20000000 == 0x20000000 {
        // Some(read_u32(&bytes[5..9], big_endian))
        // } else {
        // None
        // };

        match type_id & 0xFF {
            0x01 => {
                Ok(Geometry::Point(Point::<f64>::new(read_f64(&bytes[9..17], big_endian),
                                                     read_f64(&bytes[17..25], big_endian))))
            }
            _ => Err(Box::new(NotImplemented {})),
        }
    }
}

impl FromSqlRow<PgGeometry, Pg> for Geom {
    fn build_from_row<T: row::Row<Pg>>(row: &mut T) -> Result<Self, Box<Error + Send + Sync>> {
        Geom::from_sql(row.take())
    }
}

impl ToSql<PgGeometry, Pg> for Geom {
    fn to_sql<W: Write>(&self, _: &mut W) -> Result<IsNull, Box<Error + Send + Sync>> {
        Err(Box::new(NotImplemented {}))
    }
}

impl HasSqlType<PgGeometry> for Pg {
    fn metadata() -> PgTypeMetadata {
        PgTypeMetadata {
            oid: 25179,
            array_oid: 25185,
        }
    }
}

impl QueryId for PgGeometry {
    type QueryId = Self;
    fn has_static_query_id() -> bool {
        false
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    extern crate rustc_serialize;
    use self::rustc_serialize::hex::FromHex;
    use diesel::types::FromSql;
    use std::f64;

    #[test]
    fn read_nums() {
        assert_eq!(super::read_u32(&[0, 0, 0, 1], true), 1u32);
        assert_eq!(super::read_u32(&[1, 0, 0, 0], false), 1u32);
    }

    #[test]
    fn read_point() {
        let ewkb = "0101000020E610000000000000000045400000000000804040".from_hex().unwrap();
        let geog = Geom::from_sql(Some(&ewkb)).unwrap();
        match geog {
            Geometry::Point(p) => {
                assert!(p.x() - 42. <= f64::EPSILON);
                assert!(p.y() - 33. <= f64::EPSILON);
            }
            _ => {
                assert!(false);
            }
        }
    }
}
