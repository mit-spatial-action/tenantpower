import { appState } from "$lib/state.svelte";

const towns: Record<string, string> = {
		som: 'Somerville',
		bos: 'Boston',
		med: 'Medford',
		brk: 'Brookline',
		cam: 'Cambridge'
	};

export const USDollar = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0
});

export const muniExpand = (abb: string) => `${towns[abb] ?? 'unknown'}, MA`;

export const selectFeature = (prop_id: string) => {
        const newFeatures = appState.selected?.features.map((feature) => ({
            ...feature,
            properties: {
                ...feature.properties,
                selected: String(feature.id) === String(prop_id)
            }
        })) ?? [];

        appState.selected = {
            type: "FeatureCollection",
            features: newFeatures.toSorted((a, b) => 
                +b.properties.selected - +a.properties.selected
            )
        };
    }