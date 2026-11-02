use nisshi_sans_io::{ApiKey, Error, Frame, Header, Request, Response, RootMessageMeta};
use nisshi_service::capped_range;

/// The versions of `T`'s API that a client can reach: the range nisshi routes for it, which is
/// narrower than the protocol's own for the APIs in `CAPPED_API_VERSIONS`, since
/// `VersionGateLayer` rejects any other version before a handler sees the request.
fn valid_versions<T>() -> std::ops::RangeInclusive<i16>
where
    T: ApiKey,
{
    capped_range(T::KEY).unwrap_or_else(|| {
        RootMessageMeta::messages()
            .requests()
            .get(&T::KEY)
            .map(|meta| meta.version.valid.start..=meta.version.valid.end)
            .expect("missing metadata")
    })
}

/// Maps a fuzzed `selector` onto one of the versions of `T`'s API that a client can reach, so that every
/// target reaches every version, not only the latest.
///
/// A request and the response to it are encoded at the same version, so pick it once and pass it
/// to both [`check_request`] and [`check_response`].
pub fn api_version<T>(selector: u16) -> i16
where
    T: ApiKey,
{
    let versions = valid_versions::<T>();
    let count = u16::try_from(versions.end() - versions.start() + 1).expect("a few versions");

    versions.start() + i16::try_from(selector % count).expect("fits: below the version count")
}

/// Encodes `request` at `api_version`, decodes it, and asserts that encoding the decoded request
/// gives the same bytes again. Returns the decoded request, or `None` when `request` has no
/// encoding at that version.
///
/// The check is on bytes, not on the values. A fuzzed request is an arbitrary in-memory value,
/// and a codec isn't lossless against one: a service may fill in fields from several version eras
/// at once and trust the encoder to drop whatever doesn't apply, and a mandatory array left at
/// `None` is wire-equivalent to `Some(vec![])` but not `PartialEq`. What a codec does have to
/// satisfy is that the decoder's output is a fixed point: the bytes of what it decoded must
/// decode and encode back to those same bytes. A field the encoder skipped or wrote at the wrong
/// width shows as different bytes the second time, where comparing two decodes can't see it.
///
/// `None` means the encoder refused with [`Error::MandatoryFieldUnset`]: the value leaves a
/// field unset that is mandatory at `api_version` and nullable only at later versions, which no
/// client could have sent at this version, so there is nothing to compare. Any other error is
/// returned, and so is a mismatch, which panics.
///
/// The decoded request is what a broker would be served: a mandatory array left at `None` arrives
/// as `Some(vec![])`, and fields outside the version's range arrive at their defaults.
pub fn check_request<T>(request: T, api_version: i16) -> Result<Option<T>, Error>
where
    T: Request,
{
    let header = Header::Request {
        api_key: T::KEY,
        api_version,
        correlation_id: 0,
        client_id: Some("fuzzer".into()),
    };

    let once = match Frame::request(header.clone(), request.into()) {
        Err(Error::MandatoryFieldUnset(_)) => return Ok(None),
        encoded => encoded?,
    };

    let body = Frame::request_from_bytes(once.clone())?.body;

    // Everything the decoder returns has an encoding at this version, so this one can't refuse.
    let twice = Frame::request(header, body.clone())?;

    assert_eq!(
        once, twice,
        "re-encoding the decoded request changed its bytes"
    );

    T::try_from(body)
        .map(Some)
        .map_err(|_| Error::Message("decoded the wrong request type".into()))
}

/// The response counterpart of [`check_request`], with one difference: a response comes out of a
/// service, not out of a fuzzer, so [`Error::MandatoryFieldUnset`] is an error here. It is a
/// service that answers a client at `api_version` with a field that version requires left unset.
pub fn check_response<T>(response: T, api_version: i16) -> Result<(), Error>
where
    T: Response,
{
    let encode = |body| {
        Frame::response(
            Header::Response { correlation_id: 0 },
            body,
            T::KEY,
            api_version,
        )
    };

    let once = encode(response.into())?;
    let body = Frame::response_from_bytes(once.clone(), T::KEY, api_version)?.body;
    let twice = encode(body)?;

    assert_eq!(
        once, twice,
        "re-encoding the decoded response changed its bytes"
    );

    Ok(())
}
