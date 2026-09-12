//! Reusable compression context and incompressibility-gate policy/measurements.
use super::adaptive::PrehashedChunk;
use super::{FormatError, MAX_DECODED_RECORD_BYTES, ZSTD_LEVEL_V1};
use std::cell::RefCell;

/// Runtime evidence from the version-1 incompressibility gate.
///
/// These counters describe writer work only. They are not serialized and do
/// not authorize any Container bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IncompressibilityGateMetrics {
    pub(super) disabled_regions: usize,
    pub(super) eligible_regions: usize,
    pub(super) size_bypassed_regions: usize,
    pub(super) lz4_allowed_regions: usize,
    pub(super) lz4_rejected_regions: usize,
    pub(super) zstd1_allowed_regions: usize,
    pub(super) zstd1_rejected_regions: usize,
    pub(super) target_zstd_trials: usize,
    pub(super) target_zstd_accepted: usize,
    pub(super) target_zstd_rejected: usize,
    pub(super) raw_regions_after_gate: usize,
    pub(super) scratch_high_water_bytes: usize,
}

impl IncompressibilityGateMetrics {
    #[must_use]
    pub const fn disabled_regions(self) -> usize {
        self.disabled_regions
    }

    #[must_use]
    pub const fn eligible_regions(self) -> usize {
        self.eligible_regions
    }

    #[must_use]
    pub const fn size_bypassed_regions(self) -> usize {
        self.size_bypassed_regions
    }

    #[must_use]
    pub const fn lz4_allowed_regions(self) -> usize {
        self.lz4_allowed_regions
    }

    #[must_use]
    pub const fn lz4_rejected_regions(self) -> usize {
        self.lz4_rejected_regions
    }

    #[must_use]
    pub const fn zstd1_allowed_regions(self) -> usize {
        self.zstd1_allowed_regions
    }

    #[must_use]
    pub const fn zstd1_rejected_regions(self) -> usize {
        self.zstd1_rejected_regions
    }

    #[must_use]
    pub const fn target_zstd_trials(self) -> usize {
        self.target_zstd_trials
    }

    #[must_use]
    pub const fn target_zstd_accepted(self) -> usize {
        self.target_zstd_accepted
    }

    #[must_use]
    pub const fn target_zstd_rejected(self) -> usize {
        self.target_zstd_rejected
    }

    #[must_use]
    pub const fn raw_regions_after_gate(self) -> usize {
        self.raw_regions_after_gate
    }

    #[must_use]
    pub const fn scratch_high_water_bytes(self) -> usize {
        self.scratch_high_water_bytes
    }

    /// Adds disjoint worker or Container observations with checked counters.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ArithmeticOverflow`] if a counter cannot be
    /// represented.
    pub fn checked_merge(&mut self, other: Self) -> Result<(), FormatError> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self
                    .$field
                    .checked_add(other.$field)
                    .ok_or(FormatError::ArithmeticOverflow)?;
            };
        }
        add!(eligible_regions);
        add!(disabled_regions);
        add!(size_bypassed_regions);
        add!(lz4_allowed_regions);
        add!(lz4_rejected_regions);
        add!(zstd1_allowed_regions);
        add!(zstd1_rejected_regions);
        add!(target_zstd_trials);
        add!(target_zstd_accepted);
        add!(target_zstd_rejected);
        add!(raw_regions_after_gate);
        self.scratch_high_water_bytes = self
            .scratch_high_water_bytes
            .max(other.scratch_high_water_bytes);
        Ok(())
    }
}

/// Execution policy for the dependency-free Zstd incompressibility gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncompressibilityGatePolicy {
    /// Measurement baseline that trials target Zstd for every region.
    Off,
    /// Benchmark challenger that rejects after bounded LZ4 alone.
    Lz4Only,
    /// Bounded LZ4 plus Zstd-1 rescue policy accepted by ADR 0052.
    V1,
}

pub(super) struct AdaptiveEncoderV1 {
    zstd: zstd::bulk::Compressor<'static>,
    scratch: Box<[u8]>,
}

pub(super) fn with_adaptive_encoder_v1<T>(
    operation: impl FnOnce(&mut AdaptiveEncoderV1) -> Result<T, FormatError>,
) -> Result<T, FormatError> {
    ADAPTIVE_ENCODER_V1.with(|encoder| {
        let mut encoder = encoder.borrow_mut();
        if encoder.is_none() {
            *encoder = Some(AdaptiveEncoderV1::new()?);
        }
        operation(
            encoder
                .as_mut()
                .expect("ASSERT: worker-local adaptive encoder was initialized"),
        )
    })
}

impl AdaptiveEncoderV1 {
    fn new() -> Result<Self, FormatError> {
        let zstd =
            zstd::bulk::Compressor::new(ZSTD_LEVEL_V1).map_err(|_| FormatError::ZstdFailure)?;
        let scratch = vec![0_u8; MAX_DECODED_RECORD_BYTES].into_boxed_slice();
        Ok(Self { zstd, scratch })
    }

    pub(super) fn lz4_fits(
        &mut self,
        decoded: &[u8],
        payload_cap: usize,
    ) -> Result<bool, FormatError> {
        if payload_cap == 0 {
            return Ok(false);
        }
        let output = self
            .scratch
            .get_mut(..payload_cap)
            .ok_or(FormatError::ArithmeticOverflow)?;
        match lz4::block::compress_to_buffer(decoded, None, false, output) {
            Ok(written) => {
                assert!(
                    written <= payload_cap,
                    "ASSERT: bounded LZ4 cannot exceed its destination"
                );
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::Other => Ok(false),
            Err(_) => Err(FormatError::CompressionGateFailure),
        }
    }

    pub(super) fn zstd_fits(
        &mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<bool, FormatError> {
        Ok(self.zstd_payload(decoded, level, payload_cap)?.is_some())
    }

    pub(super) fn zstd_payload<'a>(
        &'a mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<&'a [u8]>, FormatError> {
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        let output = self
            .scratch
            .get_mut(..payload_cap)
            .ok_or(FormatError::ArithmeticOverflow)?;
        match self.zstd.context_mut().compress2(output, decoded) {
            Ok(written) => {
                assert!(
                    written <= payload_cap,
                    "ASSERT: bounded Zstd cannot exceed its destination"
                );
                Ok(Some(&output[..written]))
            }
            Err(error)
                if zstd::zstd_safe::get_error_name(error) == "Destination buffer is too small" =>
            {
                Ok(None)
            }
            Err(_) => Err(FormatError::ZstdFailure),
        }
    }

    pub(super) fn zstd_owned_payload(
        &mut self,
        decoded: &[u8],
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<Vec<u8>>, FormatError> {
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(payload_cap)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        match self.zstd.context_mut().compress2(&mut output, decoded) {
            Ok(written) => {
                // Vec's allocator may grant more capacity than requested. The
                // reduction policy cap still applies to the actual encoding.
                if written > payload_cap {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            Err(error)
                if zstd::zstd_safe::get_error_name(error) == "Destination buffer is too small" =>
            {
                Ok(None)
            }
            Err(_) => Err(FormatError::ZstdFailure),
        }
    }

    pub(super) fn zstd_fragmented_owned_payload(
        &mut self,
        chunks: &[PrehashedChunk<'_>],
        decoded_length: usize,
        level: i32,
        payload_cap: usize,
    ) -> Result<Option<Vec<u8>>, FormatError> {
        use zstd::zstd_safe::{InBuffer, OutBuffer};

        // The context keeps one Zstd frame open while each logical Chunk is
        // supplied as the next input slice. The pledged total binds the same
        // decoded length that is serialized into the Record header.
        if payload_cap == 0 {
            return Ok(None);
        }
        self.zstd
            .context_mut()
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        self.zstd
            .context_mut()
            .set_pledged_src_size(Some(
                u64::try_from(decoded_length).map_err(|_| FormatError::ArithmeticOverflow)?,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;

        let mut output = Vec::new();
        output
            .try_reserve_exact(payload_cap)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let written = {
            let mut output_buffer = OutBuffer::around(&mut output);
            for chunk in chunks {
                let mut input = InBuffer::around(chunk.bytes);
                while input.pos < input.src.len() {
                    // A previous Chunk may have consumed its complete input
                    // while filling the bounded output. Reject the trial
                    // before feeding the next Chunk into a full destination;
                    // no progress there is expected, not a codec failure.
                    if output_buffer.pos() == output_buffer.capacity() {
                        return Ok(None);
                    }
                    let input_before = input.pos;
                    let output_before = output_buffer.pos();
                    self.zstd
                        .context_mut()
                        .compress_stream(&mut output_buffer, &mut input)
                        .map_err(|_| FormatError::ZstdFailure)?;
                    if input.pos == input_before && output_buffer.pos() == output_before {
                        return Err(FormatError::ZstdFailure);
                    }
                    // Zstd output is append-only. Once the useful-payload cap
                    // is full with input left, this frame cannot beat RAW.
                    if output_buffer.pos() == output_buffer.capacity()
                        && input.pos < input.src.len()
                    {
                        return Ok(None);
                    }
                }
            }
            loop {
                let remaining = self
                    .zstd
                    .context_mut()
                    .end_stream(&mut output_buffer)
                    .map_err(|_| FormatError::ZstdFailure)?;
                if remaining == 0 {
                    break;
                }
                if output_buffer.pos() == output_buffer.capacity() {
                    return Ok(None);
                }
            }
            output_buffer.pos()
        };
        assert_eq!(
            output.len(),
            written,
            "ASSERT: Zstd initializes exactly its reported output prefix"
        );
        Ok(Some(output))
    }
}

thread_local! {
    static ADAPTIVE_ENCODER_V1: RefCell<Option<AdaptiveEncoderV1>> =
        const { RefCell::new(None) };
}

pub(super) fn compress_zstd_v1(decoded: &[u8], level: i32) -> Result<Vec<u8>, FormatError> {
    if level != ZSTD_LEVEL_V1 {
        return Err(FormatError::InvalidZstdRecord);
    }
    with_adaptive_encoder_v1(|encoder| {
        encoder
            .zstd
            .set_compression_level(level)
            .map_err(|_| FormatError::ZstdFailure)?;
        encoder
            .zstd
            .compress(decoded)
            .map_err(|_| FormatError::ZstdFailure)
    })
}
