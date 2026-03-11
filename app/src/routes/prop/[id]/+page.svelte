<script lang="ts">
	import type { PageProps } from './$types';
    import { appState } from '$lib/state.svelte';
    import PropertyCard from '$lib/components/PropertyCard.svelte';
    import type { FeatureCollection } from 'geojson';

	let { data }: PageProps & { data: FeatureCollection } = $props();

    $effect(() => {
		appState.selected = data;
	});
</script>

{#if data?.features?.length}
    <PropertyCard feature={data.features[0]} />
{/if}