const pages = {
    omsz: {
        button: document.getElementById("omszPageButton"),
        div: document.getElementById("omszPage"),
        ctl: new OmszController(),
        lastUpdate: null,
    },
    mavir: {
        button: document.getElementById("mavirPageButton"),
        div: document.getElementById("mavirPage"),
        ctl: new MavirController(),
        lastUpdate: null,
    }
}
let currentPage = pages.omsz

async function setup() {
    document.getElementById("pageLogo").src = apiUrl + 'favicon.ico'
    document.getElementById("siteLogoLink").href = apiUrl + 'favicon.ico'

    for(let page in pages) {
        await pages[page].ctl.setup()
        pages[page].button.addEventListener("click", switchPage)
    }

    let index = await fetchData(apiUrl)
    pages.omsz.lastUpdate = index.last_omsz_update
    pages.mavir.lastUpdate = index.last_mavir_update
}

async function update() {
    let index = await fetchData(apiUrl)
    if (!(index.last_omsz_update === pages.omsz.lastUpdate)) {
        await pages.mavir.ctl.updateStatus()
        pages.omsz.lastUpdate = index.last_omsz_update
    }
    if (!(index.last_mavir_update === pages.mavir.lastUpdate)) {
        await pages.mavir.ctl.updateStatus()
        pages.mavir.lastUpdate = index.last_mavir_update
        if (currentPage === pages.mavir) pages.mavir.ctl.updatePlot()
    }

    currentPage.ctl.update()
}

function switchPage(event) {
    if (event.target === currentPage.button) return;
    currentPage.div.style.display = "none"
    currentPage = pages[event.target.value]
    currentPage.div.style.display = "block"
    currentPage.ctl.switch()
}

setup().then(() => {
    setInterval(update, 10 * 1000)
})
