<script>
    import Modal from './Modal.svelte';
    import { appState } from '$lib/state.svelte';
	import { Square } from 'svelte-loading-spinners';
</script>

<Modal bind:active={appState.loading}>
    <Square size="60" color="#FF0000" unit="px" duration="1s" />
</Modal>