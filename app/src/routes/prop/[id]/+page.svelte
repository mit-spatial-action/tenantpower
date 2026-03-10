<script lang="ts">
	import type { PageProps } from './$types';
    import { appState } from '$lib/state.svelte';
    import { fly } from 'svelte/transition'

	let { data }: PageProps = $props();
    let prop = $state(data.features[0].properties)
    $effect(() => {
		appState.selected = data;
	});
    const USDollar = new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 0,
    });
</script>

<div transition:fly={{ x: -200, duration: 200 }} class="box mx-2 mt-2">
    <div class="block has-background-primary p-3">
        <h1 class="title has-text-white">{prop.prop_addr}</h1>
        <h2 class="subtitle has-text-white">{prop.town}</h2>
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
