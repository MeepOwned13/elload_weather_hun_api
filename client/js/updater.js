const omszMapFormat = {
    Temp: {
        name: langStringText('temp'),
        min: -10,
        max: 35,
        gradient: [
            [0, 212, 255, 1],
            [254, 255, 0, 1],
            [255, 128, 0, 1],
            [255, 0, 0, 1],
        ],
        measurement: '°C',
    },
    AvgTemp: {
        name: langStringText('avgTemp'),
        min: -10,
        max: 35,
        gradient: [
            [0, 212, 255, 1],
            [254, 255, 0, 1],
            [255, 128, 0, 1],
            [255, 0, 0, 1],
        ],
        measurement: '°C',
    },
    Prec: {
        name: langStringText('prec'),
        min: 0,
        max: 1.2,
        gradient: [
            [28, 189, 227, 0.05],
            [28, 189, 227, 1],
            [61, 97, 255, 1],
            [18, 0, 187, 1]
        ],
        measurement: 'mm'
    },
    RHum: {
        name: langStringText('rHum'),
        min: 0,
        max: 100,
        gradient: [
            [255, 51, 0, 1],
            [0, 255, 115, 1],
            [28, 189, 227, 1],
            [12, 0, 255, 1]
        ],
        measurement: '%'
    },
    GRad: {
        name: langStringText('gRad'),
        min: 0,
        max: 650,
        gradient: [
            [28, 189, 227, 0.5],
            [181, 208, 43, 1],
            [232, 255, 0, 1],
            [255, 179, 0, 1],
            [255, 51, 0, 1]
        ],
        measurement: 'W/m²'
    },
    AvgWS: {
        name: langStringText('avgWS'),
        min: 0,
        max: 25,
        gradient: [
            [28, 189, 227, 1],
            [0, 255, 68, 1],
            [253, 255, 0, 1],
            [255, 203, 0, 1],
            [255, 0, 0, 1]
        ],
        measurement: 'm/s',
        directionFeature: "AvgWD"
    }
}

const mavirPlotFormat = {
    // net load
    NetSystemLoad: {
        name: langStringText('NetSystemLoad'),
        color: 'rgb(102, 68, 196)',
        dash: 'solid'
    },
    NetSystemLoadFactPlantManagment: {
        name: langStringText('NetSystemLoadFactPlantManagment'),
        color: 'rgb(136, 204, 238)',
        dash: 'solid'
    },
    NetSystemLoadNetTradeSettlement: {
        name: langStringText('NetSystemLoadNetTradeSettlement'),
        color: 'rgb(68, 170, 153)',
        dash: 'solid'
    },
    NetPlanSystemLoad: {
        name: langStringText('NetPlanSystemLoad'),
        color: 'rgb(17, 119, 51)',
        dash: 'solid'
    },
    NetSystemLoadDayAheadEstimate: {
        name: langStringText('NetSystemLoadDayAheadEstimate'),
        color: 'rgb(153, 153, 51)',
        dash: 'solid'
    },
    // production
    NetPlanSystemProduction: {
        name: langStringText('NetPlanSystemProduction'),
        color: 'rgb(255, 255, 255)',
        dash: 'dashdot'
    },
    // gross load
    GrossSystemLoad: {
        name: langStringText('GrossSystemLoad'),
        color: 'rgb(204, 102, 119)',
        dash: 'dash'
    },
    GrossCertifiedSystemLoad: {
        name: langStringText('GrossCertifiedSystemLoad'),
        color: 'rgb(136, 34, 85)',
        dash: 'dash'
    },
    GrossPlanSystemLoad: {
        name: langStringText('GrossPlanSystemLoad'),
        color: 'rgb(170, 68, 153)',
        dash: 'dash'
    },
    GrossSystemLoadDayAheadEstimate: {
        name: langStringText('GrossSystemLoadDayAheadEstimate'),
        color: 'rgb(221, 204, 119)',
        dash: 'dash'
    },
}

const aiPlotFormat = {
    NetSystemLoad: {
        name: langStringText('NetSystemLoad'),
        color: 'rgb(102, 68, 196)',
        dash: 'solid'
    },
    NSLP1ago: {
        name: langStringText('NSLP1ago'),
        color: 'rgb(204, 102, 119)',
        dash: 'solid'
    },
    NSLP2ago: {
        name: langStringText('NSLP2ago'),
        color: 'rgb(17, 119, 51)',
        dash: 'solid'
    },
    NSLP3ago: {
        name: langStringText('NSLP3ago'),
        color: 'rgb(136, 204, 238)',
        dash: 'solid'
    },
}

const pages = {
    omsz: new PageController("omszPageButton", "omszPage"),
    mavir: new PageController("mavirPageButton", "mavirPage")
}
pages.omsz.addController("omsz", new OmszController(apiUrl + "omsz/", "last_omsz_update", "omszStationMapDiv",
    "omszDateInput", "omszForwardButton", "omszBackwardButton", "omszLoadingOverlay",
    omszMapFormat, 10))

pages.mavir.addController("mavir", new MavirController(apiUrl + "mavir/", "last_mavir_update", "mavirPlotDiv",
    "mavirDateInput", "mavirForwardButton", "mavirBackwardButton", "mavirLoadingOverlay",
    "load", 6, 2, mavirPlotFormat, 10))

pages.mavir.addController("s2s", new AIController(apiUrl + "ai/s2s/", "last_s2s_update", "aiPlotDiv",
    "aiDateInput", "aiForwardButton", "aiBackwardButton", "aiLoadingOverlay",
    "preds?aligned=True", 16, 6, aiPlotFormat, 60))

if (localStorage.getItem("page") === null) {
    localStorage.setItem("page", "omsz")
}
let currentPage = pages[localStorage.getItem("page")]

async function setup() {
    document.getElementById("pageLogo").src = apiUrl + 'favicon.ico'
    document.getElementById("siteLogoLink").href = apiUrl + 'favicon.ico'

    for (let key in pages) {
        pages[key].switchAway()
    }

    let index = await fetchData(apiUrl)
    for (let key in pages) {
        pages[key].setupControllers(index).then(() => {
            if (pages[key] === currentPage) currentPage.switchTo()
            pages[key].button.addEventListener("click", switchPage)
        })
    }
}

async function update() {
    let index = await fetchData(apiUrl)
    await pages.mavir.updateControllers(index)

    let updated = await pages.mavir.updateControllers(index)

    if (("mavir" in updated) && (currentPage === pages.mavir)) {
        pages.mavir.controllers["mavir"].updatePlot(true)
    }

    if (("ai" in updated) && (currentPage === pages.mavir)) {
        pages.mavir.controllers["ai"].updatePlot(true)
    }
}

function switchPage(event) {
    if (event.target === currentPage.button) return;
    currentPage.switchAway()

    currentPage = pages[event.target.value]
    localStorage.setItem("page", currentPage.button.value)
    currentPage.switchTo()
}

setup().then(() => {
    setInterval(update, 10 * 1000)
})
