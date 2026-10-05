//! HTTP message phases and field-section validation, independent of stream state.
use super::HeaderField;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Initial,
    Informational,
    Body,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RequestKind {
    #[default]
    Ordinary,
    Head,
    Connect,
}

pub(crate) struct Section {
    pub(crate) phase: Phase,
    pub(crate) content_length: Option<u64>,
    pub(crate) method: Option<RequestKind>,
    pub(crate) status: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum BodyRule {
    #[default]
    Normal,
    NoBody,
    Tunnel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Body {
    expected: Option<u64>,
    length: u64,
    rule: BodyRule,
}

impl Body {
    pub(crate) fn for_section(section: &Section, method: RequestKind) -> Self {
        let rule = if section.status.is_some()
            && (method == RequestKind::Head || matches!(section.status, Some(204 | 304)))
        {
            BodyRule::NoBody
        } else if method == RequestKind::Connect
            && section
                .status
                .is_some_and(|code| (200..300).contains(&code))
        {
            BodyRule::Tunnel
        } else {
            BodyRule::Normal
        };
        Self {
            expected: section.content_length,
            length: 0,
            rule,
        }
    }

    pub(crate) fn tunnel(&mut self) {
        self.rule = BodyRule::Tunnel;
    }

    /// Return a candidate state so callers can reject outbound operations atomically.
    pub(crate) fn checked_data(mut self, n: usize, end_stream: bool) -> Result<Self, &'static str> {
        self.length = self
            .length
            .checked_add(u64::try_from(n).map_err(|_| "body length overflow")?)
            .ok_or("body length overflow")?;
        match self.rule {
            BodyRule::NoBody if self.length != 0 => return Err("DATA on bodyless response"),
            BodyRule::Normal
                if self.expected.is_some_and(|expected| {
                    self.length > expected || (end_stream && self.length != expected)
                }) =>
            {
                return Err("content-length does not match body");
            }
            _ => {}
        }
        Ok(self)
    }
}

pub(crate) fn validate(
    headers: &[HeaderField],
    request: bool,
    phase: Phase,
    end_stream: bool,
) -> Result<Section, &'static str> {
    let trailers = phase == Phase::Body;
    if phase == Phase::End || (trailers && !end_stream) {
        return Err("trailers must end the stream");
    }
    let mut regular = false;
    let mut method = None;
    let mut scheme = None;
    let mut path = None;
    let mut authority = None;
    let mut status = None;
    let mut content_length = None;
    for h in headers {
        let name = h.name.as_bytes();
        let value = h.value.as_bytes();
        if name.is_empty()
            || value
                .iter()
                .any(|b| (*b < 0x20 && *b != b'\t') || *b == 0x7f)
            || value.first().is_some_and(|b| *b == b' ' || *b == b'\t')
            || value.last().is_some_and(|b| *b == b' ' || *b == b'\t')
        {
            return Err("invalid field name or value");
        }
        if name[0] == b':' {
            if regular || trailers {
                return Err("late or trailer pseudo-header");
            }
            let slot = match h.name.as_str() {
                ":method" if request => &mut method,
                ":scheme" if request => &mut scheme,
                ":path" if request => &mut path,
                ":authority" if request => &mut authority,
                ":status" if !request => &mut status,
                _ => return Err("unknown or wrong-role pseudo-header"),
            };
            if slot.replace(h.value.as_str()).is_some() {
                return Err("duplicate pseudo-header");
            }
        } else {
            regular = true;
            if !name.iter().all(|b| token(*b) && !b.is_ascii_uppercase()) {
                return Err("invalid or uppercase field name");
            }
            match h.name.as_str() {
                "connection" | "proxy-connection" | "keep-alive" | "transfer-encoding"
                | "upgrade" => {
                    return Err("connection-specific field");
                }
                "te" if !h.value.eq_ignore_ascii_case("trailers") => {
                    return Err("invalid TE field");
                }
                "content-length" => {
                    if trailers || value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
                        return Err("invalid content-length");
                    }
                    let n = h
                        .value
                        .parse::<u64>()
                        .map_err(|_| "invalid content-length")?;
                    if content_length.replace(n).is_some_and(|old| old != n) {
                        return Err("conflicting content-length");
                    }
                }
                _ => {}
            }
        }
    }
    if trailers {
        return Ok(Section {
            phase: Phase::End,
            content_length: None,
            method: None,
            status: None,
        });
    }
    let mut request_kind = None;
    let mut response_status = None;
    if request {
        let method = method.ok_or("missing :method")?;
        if method.is_empty() || !method.bytes().all(token) {
            return Err("invalid :method");
        }
        request_kind = Some(match method {
            "HEAD" => RequestKind::Head,
            "CONNECT" => RequestKind::Connect,
            _ => RequestKind::Ordinary,
        });
        if method == "CONNECT" {
            if authority.is_none_or(str::is_empty) || scheme.is_some() || path.is_some() {
                return Err("invalid CONNECT pseudo-headers");
            }
        } else {
            let scheme = scheme.ok_or("missing :scheme")?;
            if !scheme
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
                || !scheme
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
            {
                return Err("invalid :scheme");
            }
            let path = path.ok_or("missing :path")?;
            if path.is_empty() || path.bytes().any(|b| b <= 0x20 || b == 0x7f || b == b'#') {
                return Err("invalid :path");
            }
            if (scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
                && !(path.starts_with('/') || (path == "*" && method == "OPTIONS"))
            {
                return Err("HTTP(S) :path must be absolute or OPTIONS *");
            }
        }
    } else {
        let status = status.ok_or("missing :status")?;
        if status.len() != 3 || !status.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid :status");
        }
        let code = status.parse::<u16>().map_err(|_| "invalid :status")?;
        if !(100..=599).contains(&code) || code == 101 {
            return Err("invalid :status");
        }
        response_status = Some(code);
        if (code < 200 || code == 204) && content_length.is_some() {
            return Err("content-length on bodyless status");
        }
        if code < 200 {
            if end_stream {
                return Err("informational response ends stream");
            }
            return Ok(Section {
                phase: Phase::Informational,
                content_length: None,
                method: None,
                status: Some(code),
            });
        }
    }
    Ok(Section {
        phase: if end_stream { Phase::End } else { Phase::Body },
        content_length,
        method: request_kind,
        status: response_status,
    })
}

fn token(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}
