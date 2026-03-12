<script lang="ts">
    let { label, children } = $props();
</script>

<div class="control">
<div class="tags has-addons">
    {#if label}<span class="tag has-text-weight-bold">{label}</span>{/if}
    <span class="tag">
        {@render children?.()}
    </span>
</div>
</div>