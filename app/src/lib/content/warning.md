<script>
    import { config } from "$lib/config";
    const { canonical } = config;
    import Message from "../components/Message.svelte";
</script>

<Message header="🚨 Warning! 🚨">
    The Tenant Power database has not been updated since 2021. We are continuing to make the tool available for archival purposes, but information gleaned from this platform should be treated with __extreme__ caution.
</Message>

[Tenant Power]({canonical}) used city assessor's data from [Boston](https://www.cityofboston.gov/assessing/search/), [Cambridge](https://www.cambridgema.gov/PropertyDatabase), [Somerville](http://gis.vgsi.com/SomervilleMA/Search.aspx), [Medford](http://gis.vgsi.com/medfordma/), and [Brookline](http://apps.brooklinema.gov/assessors/propertylookup.asp), Massachusetts to identify institutional owners, and the properties they own, using machine learning. It was able to make pretty good guesses! But we encourage you to check [Massachusetts Land Records](http://www.masslandrecords.com/) and/or the assessors databases linked above to confirm ownership.

[Eric Robsky Huntley](https://ericrobskyhuntley.github.io) of the [MIT Spatial Action & Analysis Research Group](https://mit-spatial-action.github.io) built Tenant Power as part of the [Mutual Aid Medford and Somerville (MAMAS)](https://mutualaidmamas.com/) Housing Justice Working Group.

