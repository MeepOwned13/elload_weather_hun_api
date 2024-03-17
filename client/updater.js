const pages = {
    omsz: {
        button: document.getElementById("omszPageButton"),
        div: document.getElementById("omszPage"),
        updateFunc: function() {
            updateOmsz()
        },
        switchFunc: function() {
            this.updateFunc()
            updateOmszMapDimensions()
            updateOmszPlot()
        }
    },
    mavir: {
        button: document.getElementById("mavirPageButton"),
        div: document.getElementById("mavirPage"),
        updateFunc: function() {
            updateMavir()
        },
        switchFunc: function() {
            this.updateFunc()
            updateMavirPlotDimensions()
            updateMavirPlot()
        }
    }
}
let currentPage = pages.omsz

async function setup() {
    document.getElementById("pageLogo").src = apiUrl + 'favicon.ico'
    document.getElementById("siteLogoLink").href = apiUrl + 'favicon.ico'

    await updateOmszMeta()
    await updateMavirMeta()

    let index = await fetchData(apiUrl)
    omszLastUpdate = index.last_omsz_update
    mavirLastUpdate = index.last_mavir_update

    setupOmsz()
    setupMavir()

    omszPageButton.addEventListener("click", switchPage)
    mavirPageButton.addEventListener("click", switchPage)
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
        if (currentPage === pages.mavir) updateMavirPlot()
    }

    currentPage.updateFunc()
}

function switchPage(event) {
    if (event.target === currentPage.button) return;
    currentPage.div.style.display = "none"
    currentPage = pages[event.target.value]
    currentPage.div.style.display = "block"
    currentPage.switchFunc()
}

setup().then(() => {
    setInterval(update, 10 * 1000)
})
