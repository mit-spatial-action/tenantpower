<script>
    let { children } = $props();
</script>
<section class="overlay">
    <div class="columns">
        <div class="overlay-column column is-one-third">
            {@render children?.()}
        </div>
    </div>
</section>
<style>
    .overlay {
        position: absolute;
        width: 100%;
        pointer-events: none; 
    }
    :global(.overlay-column > *) {
        pointer-events: auto;
    }
</style>