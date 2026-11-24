'use strict';

/**
 * _geocode.js — Server-Side Merchant Location Geocoder
 *
 * Rules & Guardrails:
 * 1. NEVER geocode 'online' businesses -> returns null coordinates immediately (0 API calls).
 * 2. NEVER geocode street/suite addresses for 'mobile' businesses -> strictly geocodes
 *    city + state + postal_code (geocode_precision = 'city') so home addresses stay private.
 * 3. Primary: Geoapify Forward Geocoding API (server-only GEOAPIFY_API_KEY, 1 credit/call,
 *    with in-memory deduplication cache).
 * 4. Fallback: U.S. Census Bureau Public Geocoder (keyless, $0 public domain).
 * 5. Never throws or blocks merchant signup/save (5s timeout per provider).
 */

const memoryCache = new Map();

function sanitizeInputs({ address, city, state, postal_code, country = 'US', business_presence = 'physical' }) {
  const presence = String(business_presence || 'physical').toLowerCase().trim();
  if (presence === 'online') {
    return { skip: true, presence };
  }

  // Strip trailing ", CA, US" if city accidentally contains state/country
  const cleanCity = String(city || '').split(',')[0].trim();
  const cleanState = String(state || '').trim().toUpperCase();
  const rawZip = String(postal_code || '').trim();
  const zipMatch = rawZip.match(/\b(\d{5})(?:-\d{4})?\b/);
  const cleanZip = zipMatch ? zipMatch[1] : rawZip;
  const cleanCountry = String(country || 'US').trim().toUpperCase();

  // Strictly suppress street address for mobile-service businesses
  let cleanStreet = '';
  if (presence !== 'mobile' && address) {
    cleanStreet = String(address)
      .replace(/\s*(?:,|\s)\s*(?:suite|ste\.?|unit|apt\.?|apartment|bldg\.?|building|fl\.?|floor|rm\.?|room|#)\s*[a-z0-9-]+\s*$/i, '')
      .replace(/\s+#\s*[a-z0-9-]+\s*$/i, '')
      .trim();
  }

  if (!cleanStreet && !cleanCity && !cleanZip) {
    return { skip: true, presence };
  }

  return {
    skip: false,
    presence,
    cleanStreet,
    cleanCity,
    cleanState,
    cleanZip,
    cleanCountry,
  };
}

async function fetchJsonWithTimeout(url, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: { Accept: 'application/json', 'User-Agent': 'PerkfinityBackend/2.0' },
      signal: controller.signal,
    });
    if (!res.ok) return { ok: false, status: res.status, data: null };
    const data = await res.json();
    return { ok: true, status: res.status, data };
  } catch (err) {
    return { ok: false, status: 0, error: err.message, data: null };
  } finally {
    clearTimeout(timer);
  }
}

async function queryGeoapify(text, expectedState, expectedCity) {
  const apiKey = (process.env.GEOAPIFY_API_KEY || '').trim();
  if (!apiKey) return null;

  const cacheKey = `geoapify:${text.toLowerCase()}`;
  if (memoryCache.has(cacheKey)) return memoryCache.get(cacheKey);

  const url = `https://api.geoapify.com/v1/geocode/search?text=${encodeURIComponent(text)}&filter=countrycode:us&format=json&limit=1&apiKey=${encodeURIComponent(apiKey)}`;
  const { ok, data } = await fetchJsonWithTimeout(url, 5000);
  if (!ok || !data || !Array.isArray(data.results) || data.results.length === 0) {
    return null;
  }

  const top = data.results[0];
  const lat = Number(top.lat);
  const lng = Number(top.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  // Guard against wild state mismatch on synthetic/typo street addresses
  if (expectedState && expectedState.length === 2 && top.state_code) {
    if (String(top.state_code).toUpperCase() !== expectedState) {
      return null;
    }
  }

  // If we queried a street address in a known city, ensure it didn't jump across the state
  if (expectedCity && top.city) {
    const retCity = String(top.city).toLowerCase();
    const expCity = expectedCity.toLowerCase();
    // Allow close metro/suburb matches or exact match; reject if completely different county/region (>50km handled by fallback if result_type is street in wrong city)
    if (top.result_type === 'street' && retCity !== expCity && (!top.postcode || !text.includes(top.postcode))) {
      return null;
    }
  }

  let precision = 'city';
  if (top.result_type === 'building' || top.result_type === 'amenity') {
    precision = 'rooftop';
  } else if (top.result_type === 'street') {
    precision = 'street';
  } else if (top.result_type === 'postcode') {
    precision = 'zip';
  }

  const out = { latitude: lat, longitude: lng, geocode_precision: precision };
  memoryCache.set(cacheKey, out);
  return out;
}

async function queryCensusStreet(street, city, state, zip) {
  if (!street) return null;
  const oneLine = [street, city, state, zip].filter(Boolean).join(', ');
  const cacheKey = `census:${oneLine.toLowerCase()}`;
  if (memoryCache.has(cacheKey)) return memoryCache.get(cacheKey);

  const url = `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=${encodeURIComponent(oneLine)}&benchmark=Public_AR_Current&format=json`;
  const { ok, data } = await fetchJsonWithTimeout(url, 5000);
  const matches = data?.result?.addressMatches;
  if (!ok || !Array.isArray(matches) || matches.length === 0) return null;

  const coords = matches[0]?.coordinates;
  const lat = Number(coords?.y);
  const lng = Number(coords?.x);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  const out = { latitude: lat, longitude: lng, geocode_precision: 'rooftop' };
  memoryCache.set(cacheKey, out);
  return out;
}

/**
 * Geocode a MerchantLocation record safely.
 * Never throws; returns { latitude: number|null, longitude: number|null, geocode_precision: string|null }.
 */
async function geocodeLocation(input = {}) {
  try {
    const clean = sanitizeInputs(input);
    if (clean.skip) {
      return { latitude: null, longitude: null, geocode_precision: null };
    }

    const { presence, cleanStreet, cleanCity, cleanState, cleanZip } = clean;
    const cityAreaQuery = [cleanCity, cleanState, cleanZip].filter(Boolean).join(', ');

    // 1. Mobile businesses OR locations without a street address -> geocode strictly City/State/ZIP
    if (presence === 'mobile' || !cleanStreet) {
      if (!cityAreaQuery) return { latitude: null, longitude: null, geocode_precision: null };
      const cityHit = await queryGeoapify(`${cityAreaQuery}, USA`, cleanState, null);
      if (cityHit) {
        return {
          latitude: cityHit.latitude,
          longitude: cityHit.longitude,
          geocode_precision: cleanCity ? 'city' : 'zip',
        };
      }
      return { latitude: null, longitude: null, geocode_precision: null };
    }

    // 2. Physical / Hybrid businesses with a street address -> try street-level first
    const fullQuery = [cleanStreet, cleanCity, cleanState, cleanZip, 'USA'].filter(Boolean).join(', ');
    const streetHit = await queryGeoapify(fullQuery, cleanState, cleanCity);
    if (streetHit && (streetHit.geocode_precision === 'rooftop' || streetHit.geocode_precision === 'street')) {
      return streetHit;
    }

    // 3. Fallback to U.S. Census Bureau Geocoder for U.S. street addresses (e.g. if Geoapify quota/network fails or only returned city)
    const censusHit = await queryCensusStreet(cleanStreet, cleanCity, cleanState, cleanZip);
    if (censusHit) {
      return censusHit;
    }

    // 4. If Geoapify returned a valid city/postcode match earlier, use it
    if (streetHit) {
      return streetHit;
    }

    // 5. Final fallback: City / State / ZIP centroid so a valid city with a synthetic/new-build street still pins to its city
    if (cityAreaQuery) {
      const cityFallback = await queryGeoapify(`${cityAreaQuery}, USA`, cleanState, null);
      if (cityFallback) {
        return {
          latitude: cityFallback.latitude,
          longitude: cityFallback.longitude,
          geocode_precision: cleanCity ? 'city' : 'zip',
        };
      }
    }

    return { latitude: null, longitude: null, geocode_precision: null };
  } catch (err) {
    console.warn('[GEOCODE] Non-fatal geocode error:', err.message || err);
    return { latitude: null, longitude: null, geocode_precision: null };
  }
}

/**
 * Returns true ONLY if location-relevant fields changed (or coordinates are currently missing),
 * preventing wasted Geoapify credits when merchants edit offers, logos, or phone numbers.
 */
function shouldRegeocodeLocation(prevLoc, nextLoc, prevPresence, nextPresence) {
  const nextPres = String(nextPresence || prevPresence || 'physical').toLowerCase().trim();
  if (nextPres === 'online') return false;
  if (!prevLoc) return true;

  const norm = (v) => String(v || '').trim().toLowerCase();
  const prevPres = String(prevPresence || 'physical').toLowerCase().trim();

  if (prevPres !== nextPres) return true;
  if (nextPres !== 'mobile' && norm(prevLoc.address) !== norm(nextLoc.address)) return true;
  if (norm(prevLoc.city) !== norm(nextLoc.city)) return true;
  if (norm(prevLoc.state) !== norm(nextLoc.state)) return true;
  if (norm(prevLoc.postal_code) !== norm(nextLoc.postal_code)) return true;

  // Re-geocode if coordinates were never populated yet and we have at least city or zip or address
  const hasAnyAddr = Boolean(norm(nextLoc.city) || norm(nextLoc.postal_code) || (nextPres !== 'mobile' && norm(nextLoc.address)));
  if (hasAnyAddr && (prevLoc.latitude === null || prevLoc.latitude === undefined || prevLoc.longitude === null || prevLoc.longitude === undefined)) {
    return true;
  }

  return false;
}

module.exports = {
  geocodeLocation,
  shouldRegeocodeLocation,
};
