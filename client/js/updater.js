const omszMapFormat = {
    Temp: {
        name: langStringText("temp"),
        min: -10,
        max: 35,
        colorscale: [
            [0.0, 'rgb(0, 212, 255)'],
            [0.33, 'rgb(254, 255, 0)'],
            [0.67, 'rgb(255, 128, 0)'],
            [1.0, 'rgb(255, 0, 0)'],
        ],
        measurement: "°C",
    },
    AvgTemp: {
        name: langStringText("avgTemp"),
        min: -10,
        max: 35,
        colorscale: [
            [0.0, 'rgb(0, 212, 255)'],
            [0.33, 'rgb(254, 255, 0)'],
            [0.67, 'rgb(255, 128, 0)'],
            [1.0, 'rgb(255, 0, 0)'],
        ],
        measurement: "°C",
    },
    Prec: {
        name: langStringText("prec"),
        min: 0,
        max: 1.2,
        colorscale: [
            [0.0, 'rgba(28, 189, 227, 0.05)'],
            [0.33, 'rgba(28, 189, 227, 1)'],
            [0.67, 'rgba(61, 97, 255, 1)'],
            [1.0, 'rgba(18, 0, 187, 1)']
        ],
        measurement: "mm"
    },
    RHum: {
        name: langStringText("rHum"),
        min: 0,
        max: 100,
        colorscale: [
            [0.0, 'rgb(255, 51, 0)'],
            [0.33, 'rgb(0, 255, 115)'],
            [0.67, 'rgb(28, 189, 227)'],
            [1.0, 'rgb(12, 0, 255)']
        ],
        measurement: "%"
    },
    GRad: {
        name: langStringText("gRad"),
        min: 0,
        max: 650,
        colorscale: [
            [0.0, 'rgba(28, 189, 227, 0.5)'],
            [0.25, 'rgba(181, 208, 43, 1)'],
            [0.5, 'rgba(232, 255, 0, 1)'],
            [0.75, 'rgba(255, 179, 0, 1)'],
            [1.0, 'rgba(255, 51, 0, 1)']
        ],
        measurement: "W/m²"
    },
    AvgWS: {
        name: langStringText("avgWS"),
        min: 0,
        max: 25,
        colorscale: [
            [0.0, 'rgb(28, 189, 227)'],
            [0.25, 'rgb(0, 255, 68)'],
            [0.5, 'rgb(253, 255, 0)'],
            [0.75, 'rgb(255, 203, 0)'],
            [1.0, 'rgb(255, 0, 0)']
        ],
        measurement: "m/s",
        directionFeature: "AvgWD"
    }
}

const mavirPlotFormat = {
    // net load
    NetSystemLoad: {
        name: langStringText("NetSystemLoad"),
        color: "rgb(102, 68, 196)",
        dash: "solid"
    },
    NetSystemLoadFactPlantManagment: {
        name: langStringText("NetSystemLoadFactPlantManagment"),
        color: "rgb(136, 204, 238)",
        dash: "solid"
    },
    NetSystemLoadNetTradeSettlement: {
        name: langStringText("NetSystemLoadNetTradeSettlement"),
        color: "rgb(68, 170, 153)",
        dash: "solid"
    },
    NetPlanSystemLoad: {
        name: langStringText("NetPlanSystemLoad"),
        color: "rgb(17, 119, 51)",
        dash: "solid"
    },
    NetSystemLoadDayAheadEstimate: {
        name: langStringText("NetSystemLoadDayAheadEstimate"),
        color: "rgb(153, 153, 51)",
        dash: "solid"
    },
    // production
    NetPlanSystemProduction: {
        name: langStringText("NetPlanSystemProduction"),
        color: "rgb(255, 255, 255)",
        dash: "dashdot"
    },
    // gross load
    GrossSystemLoad: {
        name: langStringText("GrossSystemLoad"),
        color: "rgb(204, 102, 119)",
        dash: "dash"
    },
    GrossCertifiedSystemLoad: {
        name: langStringText("GrossCertifiedSystemLoad"),
        color: "rgb(136, 34, 85)",
        dash: "dash"
    },
    GrossPlanSystemLoad: {
        name: langStringText("GrossPlanSystemLoad"),
        color: "rgb(170, 68, 153)",
        dash: "dash"
    },
    GrossSystemLoadDayAheadEstimate: {
        name: langStringText("GrossSystemLoadDayAheadEstimate"),
        color: "rgb(221, 204, 119)",
        dash: "dash"
    },
}

const aiPlotFormat = {
    NetSystemLoad: {
        name: langStringText("NetSystemLoad"),
        color: "rgb(102, 68, 196)",
        dash: "solid"
    },
    NSLP1ago: {
        name: langStringText("NSLP1ago"),
        color: "rgb(204, 102, 119)",
        dash: "solid"
    },
    NSLP2ago: {
        name: langStringText("NSLP2ago"),
        color: "rgb(17, 119, 51)",
        dash: "solid"
    },
    NSLP3ago: {
        name: langStringText("NSLP3ago"),
        color: "rgb(136, 204, 238)",
        dash: "solid"
    },
}

const pages = {
    omsz: new PageController("omszPageButton", "omszPage"),
    mavir: new PageController("mavirPageButton", "mavirPage")
}
pages.omsz.addController("omsz", new OmszController(apiUrl + "omsz/", "omszContainer", "last_omsz_update",
    "omszUrlA", omszMapFormat, 10))

pages.mavir.addController("mavir", new MavirController(apiUrl + "mavir/", "mavirContainer", "last_mavir_update",
    "mavirUrlA", "load", 6, 2, mavirPlotFormat, 10))

pages.mavir.addController("s2s", new AIController(apiUrl + "ai/s2s/", "aiContainer", "last_s2s_update",
    "preds?aligned=True", 16, 6, aiPlotFormat, 60))

if (localStorage.getItem("page") === null) {
    localStorage.setItem("page", "omsz")
}
let currentPage = pages[localStorage.getItem("page")]

/**
* Sets up website, displaying page read from localStorage
* @async
*/
async function setup() {
    document.getElementById("pageLogo").src = apiUrl + "favicon.ico"
    document.getElementById("siteLogoLink").href = apiUrl + "favicon.ico"

    for (let key in pages) {
        pages[key].switchAway()
    }

    let index = await fetchData(apiUrl)
    for (let key in pages) {
        pages[key].setupControllers(index).then(() => {
            if (pages[key] === currentPage) {
                currentPage.switchTo()
                currentPage.button.classList.add("onpage")
            }
            pages[key].button.addEventListener("click", switchPage)
        })
    }
}

/**
* Updates PageControllers and PlotControllers
* @async
*/
async function update() {
    let index = await fetchData(apiUrl)
    await pages.omsz.updateControllers(index)

    let updated = await pages.mavir.updateControllers(index)

    if (("mavir" in updated) && (currentPage === pages.mavir)) {
        await pages.mavir.controllers["mavir"].updatePlot(true)
    }

    if (("s2s" in updated) && (currentPage === pages.mavir)) {
        await pages.mavir.controllers["s2s"].updatePlot(true)
    }
}

/**
* Event added to each page switch button to facilitate page switching
* Hides current page and displays new one, calling necessary functions
*/
function switchPage(event) {
    if (event.target === currentPage.button) return;
    currentPage.button.classList.remove("onpage")
    currentPage.switchAway()

    currentPage = pages[event.target.value]
    currentPage.button.classList.add("onpage")
    localStorage.setItem("page", currentPage.button.value)
    currentPage.switchTo()
}

setup().then(() => {
    setInterval(update, 10 * 1000)
})
