use hashbrown::{HashMap, hash_map::RawEntryMut};
use log::{debug, info, trace};
use rustc_hash::FxBuildHasher;
use std::{mem, ptr, slice, str};

pub fn onebrc(data: &[u8], num_threads: usize) -> String {
    let segments = segments(data, num_threads);

    let mut db: HashMap<&str, Station, FxBuildHasher> = HashMap::default();

    std::thread::scope(|s| {
        let mut thread_handles = Vec::with_capacity(segments.len());
        for (i, segment) in segments.iter().enumerate() {
            let handle = std::thread::Builder::new()
                .name(format!("Worker [{:3}]", i))
                .spawn_scoped(s, move || process_segment(data, *segment))
                .unwrap();
            thread_handles.push(handle);
        }
        for handle in thread_handles {
            let part = handle.join().unwrap();
            part.iter().for_each(|(k, v)| {
                let entry = db.raw_entry_mut().from_key(k);
                match entry {
                    RawEntryMut::Occupied(mut o) => {
                        o.get_mut().merge(v);
                    }
                    RawEntryMut::Vacant(x) => {
                        x.insert(k, *v);
                    }
                }
            });
        }
    });

    calculate_outstring(db)
}

/// accumulates all measurements in segment in a Hashmap
pub fn process_segment(data: &[u8], segment: Segment) -> HashMap<&str, Station, FxBuildHasher> {
    const SEMICOLON_MASK: u64 = 0x3B3B3B3B3B3B3B3B;
    // Initialization
    let mut db: HashMap<&str, Station, FxBuildHasher> = HashMap::default();
    let mut read_offset = segment.start;
    let mut line_beginning = segment.start;

    // read and parse loop
    info!(
        "{:12} started parsing segment {{{}, {}}}",
        std::thread::current().name().unwrap_or("Main Thread"),
        segment.start,
        segment.end
    );
    while line_beginning < segment.end {
        // get 8 bytes and look for first b';'
        let bytes: u64 =
            unsafe { ptr::read_unaligned(data.as_ptr().add(read_offset) as *const u64) };
        trace!(
            "{:12} read 8 bytes: {:#019x} at index: {read_offset}",
            std::thread::current().name().unwrap_or("Main Thread"),
            bytes
        );
        #[cfg(debug_assertions)]
        debug_u64_bytes(bytes, read_offset);

        let separator_pos = find_separator(bytes, SEMICOLON_MASK);
        // if there is no b';' continue with next byte
        if 8 == separator_pos {
            read_offset += mem::align_of::<u64>();
            debug!(
                "{:12} no separator found in current byte, continuing at index: {}",
                std::thread::current().name().unwrap_or("Main Thread"),
                read_offset
            );
            continue;
        }
        trace!(
            "{:12} found semicolon (;) at byte: {separator_pos}",
            std::thread::current().name().unwrap_or("Main Thread")
        );

        // get zero-cost string slice of station name
        let str_len = read_offset + separator_pos - line_beginning;
        let station_name = unsafe {
            str::from_utf8_unchecked(slice::from_raw_parts(
                data.as_ptr().add(line_beginning),
                str_len,
            ))
        };
        debug!(
            "{:12} parsed station name: {station_name} from index: {line_beginning} and length: {str_len}",
            std::thread::current().name().unwrap_or("Main Thread")
        );

        // parse temperature and get new line_beginning;
        let temp_bytes = unsafe {
            ptr::read_unaligned(data.as_ptr().add(read_offset + separator_pos + 1) as *const i64)
        };
        trace!(
            "{:12} read 8 bytes: {:#018x} at index: {}",
            std::thread::current().name().unwrap_or("Main Thread"),
            temp_bytes,
            read_offset + separator_pos + 1
        );
        #[cfg(debug_assertions)]
        debug_u64_bytes(temp_bytes.cast_unsigned(), read_offset + separator_pos + 1);
        let temp_tup = parse_temp_int(temp_bytes);
        let temp = temp_tup.0; // temparature
        debug!(
            "{:12} parsed temperature: {:+5.1} from index: {}",
            std::thread::current().name().unwrap_or("Main Thread"),
            temp as f64 / 10.0,
            read_offset + separator_pos + 1
        );
        info!(
            "{:12} measured temperature: {:+5.1} at Station: {station_name}",
            std::thread::current().name().unwrap_or("Main Thread"),
            temp as f64 / 10.0
        );

        // make db entry
        let entry = db.raw_entry_mut().from_key(station_name);
        match entry {
            RawEntryMut::Occupied(mut o) => {
                o.get_mut().update(temp);
            }
            RawEntryMut::Vacant(v) => {
                v.insert(station_name, Station::new(temp));
            }
        }

        line_beginning = read_offset + separator_pos + temp_tup.1 + 1; // temp_tup.1 = next line start
        debug!(
            "{:12} Next line starts at index: {line_beginning}",
            std::thread::current().name().unwrap_or("Main Thread"),
        );
        read_offset = line_beginning;
    }
    info!(
        "{:12} parsed all entries in segment {{{}, {}}}",
        std::thread::current().name().unwrap_or("Main Thread"),
        segment.start,
        segment.end
    );
    db
}

#[inline]
/// parses the temperature from the byte after `semicolon_index` and also gets offset of next line
/// start
/// # Safety
/// the file backing data must be 8 bytes longer than data to avoid reading OOB.
pub fn parse_temp_int(number_bytes: i64) -> (i64, usize) {
    const DOT_BITS: i64 = 0x10101000;
    const MAGIC_MULTIPLIER: i64 = 100 * 0x0100_0000 + 10 * 0x01_0000 + 1;

    let inv_number_bytes = !number_bytes;

    let dot_position = (inv_number_bytes & DOT_BITS).trailing_zeros();

    // calculate the sign
    let signed = (inv_number_bytes << 59) >> 63;
    let shiftwidth = dot_position ^ 0b11100;
    let minus_filter = !(signed & 0xFF);
    // use pre-calculated decimal position to adjust the values
    let digits = ((number_bytes & minus_filter).unbounded_shl(shiftwidth)) & 0x0F000F0F00i64;
    // multiply by *magic* to get result
    let abs_value: i64 = ((digits.wrapping_mul(MAGIC_MULTIPLIER)) >> 32) & 0x3FF;

    // (parsed_int, start_next_line)
    (
        (abs_value + signed) ^ signed, // add sign back to the absolute value
        ((dot_position >> 3) + 3) as usize,
    )
}

#[inline]
/// returns the byte number starting at 1 in which separator was found. 0 if not found.
/// maybe use option to return index instead of byte number
///
/// `segment`: bytes to look for separator in
/// `separator_mask`: bytemask of value to look for
pub fn find_separator(segment: u64, separator_mask: u64) -> usize {
    let comparison = segment ^ separator_mask; // find difference. matching byte is now 0b0000_0000
    let lsb = comparison.wrapping_sub(0x0101010101010101u64); // subtract 1 from each byte. matching byte is now 0b1111_1111;
    let msb = (!comparison) & 0x8080808080808080u64; // and with inverse of difference. everything but matching byte is now less than 0b1111_1111
    let mask = lsb & msb; // and with highest bit per byte. only byte that matches pattern is now 0b100_0000
    (mask.trailing_zeros() >> 3) as usize // divide by 8 to get position of matching byte in the segment
}

/// calculates min/mean/max from data in the HashMap and returns the String with format:
/// {station_name=min/mean/max, ...}
pub fn calculate_outstring(db: HashMap<&str, Station, FxBuildHasher>) -> String {
    let mut out_string: String = '{'.to_string();
    let mut ordered = db.into_iter().collect::<Vec<(&str, Station)>>();
    ordered.sort_by(|a, b| a.0.cmp(b.0));
    for (k, v) in ordered {
        let (min, mean, max) = (
            (v.min) as f64 / 10.0,
            f64::round(v.sum as f64 / v.count as f64) / 10.0,
            (v.max) as f64 / 10.0,
        );
        out_string = format!("{out_string}{k}={min:.1}/{mean:.1}/{max:.1}, ");
    }
    if out_string.len() > 2 {
        out_string.pop();
        out_string.pop();
    }
    out_string.push('}');
    out_string.push('\n');
    out_string
}

/// divides the provided data up into `num_segments` slices of roughly the same size.
/// splits happen after the closest `b'\n'` in front of the perfect split
pub fn segments(data: &[u8], num_segments: usize) -> Vec<Segment> {
    let size = data.len();
    // corner case for small data to avoid index out of bounds
    if size < 107 * num_segments {
        vec![Segment {
            start: 0,
            end: size,
        }]
    } else {
        let mut segments: Vec<Segment> = Vec::with_capacity(num_segments);
        let mut start = 0;
        for i in 1..=num_segments {
            let end = if i == num_segments {
                size
            } else {
                let mut index = 0;
                for j in 0..107 {
                    index = i * (size / num_segments) - j;
                    if b'\n' == data[index] {
                        break;
                    }
                }
                index
            };
            segments.push(Segment { start, end });
            start = end + 1;
        }
        segments
    }
}

#[derive(Clone, Copy)]
pub struct Segment {
    start: usize,
    end: usize,
}

#[derive(Clone, Copy)]
pub struct Station {
    sum: i64,
    count: i64,
    min: i64,
    max: i64,
}
impl Station {
    #[inline]
    fn new(temp: i64) -> Self {
        Self {
            sum: temp,
            count: 1,
            min: temp,
            max: temp,
        }
    }

    #[inline]
    fn update(&mut self, temp: i64) {
        self.sum += temp;
        self.count += 1;
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
    }

    #[inline]
    pub fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}

#[cfg(debug_assertions)]
fn debug_u64_bytes(word: u64, base_offset: usize) -> String {
    let bytes = word.to_le_bytes();
    let indices: Vec<String> = (0..8)
        .map(|i| format!("[{:3}]", base_offset + i))
        .rev()
        .collect();
    let chars: Vec<String> = bytes
        .iter()
        .map(|&b| match b {
            b'\n' => " '\\n'".to_string(),
            b'\t' => " '\\t'".to_string(),
            b'\r' => " '\\r'".to_string(),
            b'\0' => " '\\0'".to_string(),
            b if b.is_ascii_graphic() || b == b' ' => format!("  {:?}", b as char),
            b => format!(" 0x{b:02X}"),
        })
        .rev()
        .collect();
    format!("{}\n{}", indices.join(" "), chars.join(" "))
}

#[allow(unused)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::prelude::*;
    use std::sync::Once;
    use std::{fs, fs::File};

    static INIT: Once = Once::new();

    fn init_logger() {
        INIT.call_once(|| {
            let log_file = fs::File::create("logs/tests.log").expect("failed to create log file");
            env_logger::Builder::new()
                .target(env_logger::Target::Pipe(Box::new(log_file)))
                .is_test(true)
                .filter_level(log::LevelFilter::Debug)
                .format_timestamp(None)
                .init();
        });
    }

    macro_rules! new_tests {
        ($($sample_name:ident),+) => {$(
            #[test]
            fn $sample_name() {
                init_logger();
                let mut padding = vec![0u8; 8];
                let padding_len = padding.len();
                let correct_file = fs::read(concat!(
                    "resources/samples/",
                    stringify!($sample_name),
                    ".out"
                ))
                .unwrap();
                let mut measurements_file = fs::File::open(concat!(
                    "resources/samples/",
                    stringify!($sample_name),
                    ".txt"
                ))
                .unwrap();
                let mut measurements_data: Vec<u8> = Vec::with_capacity(measurements_file.metadata().unwrap().len() as usize + 8);
                measurements_file.read_to_end(&mut measurements_data);
                measurements_data.append(&mut padding);

                let res = onebrc(&measurements_data[..measurements_data.len()-8], 1);
                let correct_file_str = String::from_utf8(correct_file).unwrap();
                assert_eq!(correct_file_str, res);
            })+};
    }

    new_tests!(
        measurements_1,
        measurements_2,
        measurements_3,
        measurements_10,
        measurements_20,
        measurements_dot,
        measurements_short,
        measurements_shortest,
        measurements_rounding,
        measurements_boundaries,
        measurements_complex_utf8,
        measurements_10000_unique_keys
    );

    //macro_rules! impl_tests {
    //    (@inner $func_name:ident $sample_name:ident) => {
    //        paste! {
    //            #[test]
    //            fn [< $func_name _ $sample_name >]() {
    //                let mut append = vec![0u8;8];
    //                let correct_file = fs::read(concat!("resources/samples/", stringify!($sample_name), ".out"))
    //                    .unwrap();
    //                let mut measurements_file = fs::read(concat!("resources/samples/", stringify!($sample_name), ".txt"))
    //                    .unwrap();
    //                measurements_file.append(&mut append);
    //
    //                    let db = $func_name(&measurements_file, Segment {start:0, end: measurements_file.len()-append.len()});
    //                    let res = calculate_outstring(db);
    //                    let correct_file_str = std::str::from_utf8(&correct_file).unwrap().to_string();
    //                    assert_eq!(correct_file_str, res);
    //                }
    //            }
    //        };
    //        (@inner $func_name:ident | [$($sample_name:ident),+]) => {
    //            $(impl_tests!(@inner $func_name $sample_name);)+
    //        };
    //        ($($func_name:ident),+ | $samples:tt) => {
    //            $(impl_tests!(@inner $func_name | $samples);)+
    //        };
    //    }
    //
    // macro!(function, function, function | sample, sample, sample)
    //
    //    impl_tests!(
    //        process_segment
    //            | [
    //                measurements_1,
    //                measurements_2,
    //                measurements_3,
    //                measurements_10,
    //                measurements_20,
    //                measurements_dot,
    //                measurements_short,
    //                measurements_shortest,
    //                measurements_rounding,
    //                measurements_boundaries,
    //                measurements_complex_utf8,
    //                measurements_10000_unique_keys
    //            ]
    //    );
}
