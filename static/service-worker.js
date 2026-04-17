const CACHE_NAME = 'dashboard-sw-v5';
const OFFLINE_URL = '/';
const STATIC_URLS = [
  '/',
  '/command-center',
  '/market-intelligence',
  '/ai-predictions',
  '/whale-intelligence',
  '/sentiment-intelligence',
  '/risk-dashboard',
  '/strategy-lab',
  '/system-monitor',
  '/order-flow',
  '/intelligence-alerts',
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(STATIC_URLS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))
    ))
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') {
    return;
  }

  const url = new URL(request.url);
  const isApiRequest = url.pathname.startsWith('/api/');
  const isDashboardAuthScript = url.pathname === '/static/dashboard_auth.js';
  const shouldBypassCache =
    isApiRequest ||
    url.pathname === '/service-worker.js' ||
    url.pathname === '/manifest.webmanifest';
  const shouldNetworkFirst = request.destination === 'document' || isDashboardAuthScript;

  if (shouldBypassCache) {
    return;
  }

  if (shouldNetworkFirst) {
    event.respondWith(
      fetch(request)
        .then(networkResponse => {
          if (networkResponse && networkResponse.status === 200) {
            const clone = networkResponse.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
          }
          return networkResponse;
        })
        .catch(() => caches.match(request).then(cached => cached || caches.match(OFFLINE_URL)))
    );
    return;
  }

  event.respondWith(
    caches.match(request).then(cached => {
      return cached || fetch(request).then(networkResponse => {
        if (networkResponse && networkResponse.status === 200) {
          const clone = networkResponse.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
        }
        return networkResponse;
      });
    })
  );
});
