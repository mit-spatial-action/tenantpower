import type { FeatureCollection } from 'geojson';

export const appState = $state({
    isGeocoding: false as boolean,
    errors: {
        geocode: false as boolean
    },
    isInit: true as boolean,
    selected: null as FeatureCollection | null
});