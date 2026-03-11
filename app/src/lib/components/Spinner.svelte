<script>
    import { appState } from '$lib/state.svelte';
    import { fade, fly } from "svelte/transition";
    import { navigating } from '$app/state';
	import { Square } from 'svelte-loading-spinners';

    let spinning = $derived(navigating.to !== null || appState.isGeocoding);
</script>

{#if spinning}
<div class="modal is-active" transition:fade={{ duration: 200 }}>
    <div class="modal-background"></div>
    <div transition:fly={{ y: 300, duration: 300 }}>
        <Square size="60" color="#FF0000" unit="px" duration="1s" />
    </div>
</div>
{/if}