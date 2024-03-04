async function setup() {
    await updateOmszMeta()
    await updateMavirMeta()

    let index = await fetchData(apiUrl)
    omszLastUpdate = index.last_omsz_update
    mavirLastUpdate = index.last_mavir_update

    setupOmsz()
    setupMavir()
}

async function update() {
    let index = await fetchData(apiUrl)
    if (!(index.last_omsz_update === omszLastUpdate)) {
        await updateOmszMeta()
        omszLastUpdate = index.last_omsz_update
    }
    if (!(index.last_mavir_update === mavirLastUpdate)) {
        await updateMavirMeta()
        mavirLastUpdate = index.last_mavir_update
    }

    updateOmsz()
    updateMavir()
}

setup().then(() =>{
    setInterval(update, 10 * 1000)
})
