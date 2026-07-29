<?php
/**
 * Plugin Name: tana
 * Description: Serves wp-content/uploads from a tana store, and hands WooCommerce downloads out as short-lived signed links.
 * Version:     0.1.0
 * License:     MIT
 *
 * Drop this in wp-content/mu-plugins/ and configure it in wp-config.php.
 *
 * This plugin is OPTIONAL. tana works without it: the agent mounts the
 * uploads directory, WordPress writes files there as it always has,
 * and the web server serves them from the mount. What the plugin adds
 * is worth having anyway:
 *
 *   - Public media URLs point at the store (or a CDN in front of it)
 *     instead of at the web server, so public traffic never touches
 *     the mount. That matters more than it sounds: a read arriving
 *     through the mount can pull an evicted object back onto local
 *     disk, and a crawler walking an old archive would refill the
 *     cache with exactly what was evicted for being cold.
 *
 *   - WooCommerce downloads become presigned links with a short life,
 *     rather than files streamed through PHP. The store refuses those
 *     keys to anonymous callers whatever this plugin does — that rule
 *     lives in tana's configuration, where nobody can switch it off
 *     from wp-admin — so a signed link is the only way through.
 *
 * wp-config.php:
 *
 *   define('TANA_ENDPOINT',   'http://store01.lan:9200');
 *   define('TANA_BUCKET',     'shop-uploads');
 *   define('TANA_REGION',     'tana');
 *   define('TANA_ACCESS_KEY', '...');
 *   define('TANA_SECRET_KEY', '...');
 *   // Where browsers should fetch public media. A CDN, a reverse
 *   // proxy, or the store itself. No trailing slash.
 *   define('TANA_PUBLIC_BASE', 'https://cdn.example.com/shop-uploads');
 *   // How long a download link stays valid. Long enough to start a
 *   // download on a slow connection, short enough that a link pasted
 *   // in a forum is worthless by the time anyone clicks it.
 *   define('TANA_PRESIGN_TTL', 300);
 *
 * WooCommerce integration was written against WooCommerce 8.x and 9.x.
 * Verify it against your version before trusting it with paid files:
 * place an order, download it, and confirm the link expires.
 */

if (!defined('ABSPATH')) {
    exit;
}

if (!defined('TANA_PRESIGN_TTL')) {
    define('TANA_PRESIGN_TTL', 300);
}

/**
 * Whether the plugin has everything it needs. Missing configuration
 * disables it rather than half-enabling it: a site serving broken
 * image URLs is worse than a site serving them the old way.
 */
function tana_configured(): bool
{
    foreach (['TANA_ENDPOINT', 'TANA_BUCKET', 'TANA_REGION', 'TANA_ACCESS_KEY', 'TANA_SECRET_KEY'] as $c) {
        if (!defined($c) || constant($c) === '') {
            return false;
        }
    }
    return true;
}

/**
 * The base URL browsers should use for public media.
 */
function tana_public_base(): string
{
    if (defined('TANA_PUBLIC_BASE') && TANA_PUBLIC_BASE !== '') {
        return rtrim(TANA_PUBLIC_BASE, '/');
    }
    return rtrim(TANA_ENDPOINT, '/') . '/' . TANA_BUCKET;
}

/**
 * Percent-encodes per RFC 3986, which is what the signature expects.
 * rawurlencode already matches on modern PHP; the wrapper exists so
 * the intent is explicit at every call site.
 */
function tana_uri_encode(string $s): string
{
    return rawurlencode($s);
}

/**
 * Encodes an object key as a URL path, keeping the separators.
 */
function tana_encode_key(string $key): string
{
    return implode('/', array_map('tana_uri_encode', explode('/', $key)));
}

/**
 * Builds a presigned URL for an object.
 *
 * This is AWS Signature Version 4 in its query-string form. The
 * signature covers the method, the path, the query and the expiry, so
 * a link cannot be edited into a link for something else: changing the
 * key, the method or the deadline invalidates it.
 */
function tana_presign(string $key, ?int $ttl = null, string $method = 'GET'): string
{
    $ttl   = $ttl ?? (int) TANA_PRESIGN_TTL;
    $now   = gmdate('Ymd\THis\Z');
    $date  = substr($now, 0, 8);
    $scope = $date . '/' . TANA_REGION . '/s3/aws4_request';

    $parts = parse_url(TANA_ENDPOINT);
    $host  = $parts['host'];
    if (isset($parts['port'])) {
        $host .= ':' . $parts['port'];
    }

    $path = '/' . tana_uri_encode(TANA_BUCKET) . '/' . tana_encode_key($key);

    $query = [
        'X-Amz-Algorithm'     => 'AWS4-HMAC-SHA256',
        'X-Amz-Credential'    => TANA_ACCESS_KEY . '/' . $scope,
        'X-Amz-Date'          => $now,
        'X-Amz-Expires'       => (string) $ttl,
        'X-Amz-SignedHeaders' => 'host',
    ];
    ksort($query);

    $canonicalQuery = [];
    foreach ($query as $k => $v) {
        $canonicalQuery[] = tana_uri_encode($k) . '=' . tana_uri_encode($v);
    }
    $canonicalQuery = implode('&', $canonicalQuery);

    $canonical = implode("\n", [
        $method,
        $path,
        $canonicalQuery,
        'host:' . $host . "\n",
        'host',
        'UNSIGNED-PAYLOAD',
    ]);

    $stringToSign = implode("\n", [
        'AWS4-HMAC-SHA256',
        $now,
        $scope,
        hash('sha256', $canonical),
    ]);

    // Each step narrows what the derived key can sign: a key stolen
    // today cannot sign tomorrow's requests, nor another region's.
    $k = hash_hmac('sha256', $date, 'AWS4' . TANA_SECRET_KEY, true);
    $k = hash_hmac('sha256', TANA_REGION, $k, true);
    $k = hash_hmac('sha256', 's3', $k, true);
    $k = hash_hmac('sha256', 'aws4_request', $k, true);

    $query['X-Amz-Signature'] = hash_hmac('sha256', $stringToSign, $k);

    $out = [];
    foreach ($query as $key2 => $value) {
        $out[] = tana_uri_encode($key2) . '=' . tana_uri_encode($value);
    }
    return rtrim(TANA_ENDPOINT, '/') . $path . '?' . implode('&', $out);
}

/**
 * Turns a local uploads path or URL into an object key.
 *
 * Returns null for anything outside the uploads directory, which is
 * how the filters below decline to touch what is not theirs.
 */
function tana_key_from(string $pathOrUrl): ?string
{
    $uploads = wp_get_upload_dir();

    foreach ([$uploads['basedir'], $uploads['baseurl']] as $prefix) {
        if ($prefix === '' || strpos($pathOrUrl, $prefix) !== 0) {
            continue;
        }
        $key = ltrim(substr($pathOrUrl, strlen($prefix)), '/\\');
        return str_replace('\\', '/', $key);
    }
    return null;
}

// ---------------------------------------------------------------------
// Public media: point browsers at the store instead of the web server.
// ---------------------------------------------------------------------

if (tana_configured() && !is_admin()) {
    /**
     * Rewrites the uploads base URL. Everything WordPress builds on
     * top of it — attachment URLs, srcset entries, the media library
     * front end — follows from this one value.
     */
    add_filter('upload_dir', function (array $dirs): array {
        $base = tana_public_base();
        $dirs['baseurl'] = $base;
        $dirs['url']     = $base . ($dirs['subdir'] ?? '');
        return $dirs;
    }, 20);
}

// ---------------------------------------------------------------------
// WooCommerce downloads: hand out signed links with a short life.
// ---------------------------------------------------------------------

if (tana_configured()) {
    /**
     * Ask WooCommerce to redirect rather than stream the file through
     * PHP. Streaming a large file through a PHP-FPM worker holds that
     * worker for the whole download, and a shop with a dozen customers
     * downloading at once is a shop that has run out of workers.
     */
    add_filter('woocommerce_file_download_method', function ($method) {
        return 'redirect';
    }, 20);

    /**
     * Replace the stored file path with a presigned URL.
     *
     * The link is minted per request and expires. tana refuses these
     * keys to anonymous callers regardless — the protected list lives
     * in the store's configuration — so a link that has expired is a
     * link that does nothing, and a link that leaks is worthless
     * shortly afterwards.
     */
    add_filter('woocommerce_product_file_download_path', function ($filePath, $product = null, $downloadId = null) {
        if (!is_string($filePath) || $filePath === '') {
            return $filePath;
        }
        $key = tana_key_from($filePath);
        if ($key === null) {
            // Not ours: a file hosted elsewhere, or an external URL.
            return $filePath;
        }
        return tana_presign($key);
    }, 20, 3);
}
