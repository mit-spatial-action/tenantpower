<script lang="ts">
	import type { PageProps } from './$types';
    import { appState } from '$lib/state.svelte';
    import { fly } from 'svelte/transition';
    import type { FeatureCollection } from 'geojson';

	let { data }: PageProps = $props();
    
    $effect(() => {
		appState.selected = data as FeatureCollection;
	});
    const prop = $derived(data?.features[0]?.properties ?? null);

    const USDollar = new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 0,
    });
    const muniExpand = (abb:string): string => {
        const towns: Record<string, string> = {
            som: "Somerville",
            bos: "Boston",
            med: "Medford",
            brk: "Brookline",
            cam: "Cambridge"
        };
        return `${towns[abb] ?? "unknown"}, MA`
    }
</script>

{#if prop}
<div transition:fly={{ x: -200, duration: 200 }} class="bordered box mx-2 mt-2">
    <div class="block has-background-primary p-3">
        <h1 class="title has-text-white">{prop.prop_addr}</h1>
        <h2 class="subtitle has-text-white">{muniExpand(prop.town)}</h2>
    </div>
    <div class="block">
        <div class="field is-grouped is-grouped-multiline">
            {#if prop.year}
            <div class="control">
                <div class="tags has-addons">
                    <span class="tag">Year</span>
                    <span class="tag is-primary">{prop.year}</span>
                </div>
            </div>
            {/if}
            {#if prop.ass_val}
            <div class="control">
                <div class="tags has-addons">
                    <span class="tag">Value</span>
                    <span class="tag is-primary">{USDollar.format(prop.ass_val)}</span>
                </div>
            </div>
            {/if}
        </div>
        <div class="card">
            <div class="card-header">
                <p class="card-header-title">INDIAN TRUST LLC</p>
            </div>
            <div class="card-content">
                <div class="content">
                yes?
                </div>
            </div>
        </div>
    </div>
    <div class="block">
        <p class="buttons">
            <a class="button is-primary" href="/cluster/{prop.clst}">
                <span class="icon">
                    <iconify-icon icon="mdi:home"></iconify-icon>
                </span>
                <span>What Else Do They Own?</span>
            </a>
            <button class="button is-primary">
                <span class="icon">
                    <iconify-icon icon="mdi:home"></iconify-icon>
                </span>
                <span>What's Nearby?</span>
            </button>
        </p>
    </div>
</div>
{/if}