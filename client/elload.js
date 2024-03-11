// global constants
const mavirUpdateButton = document.getElementById("mavirUpdateButton")
const mavirForwardButton = document.getElementById("mavirForwardButton")
const mavirBackwardButton = document.getElementById("mavirBackwardButton")
const mavirDateInput = document.getElementById("mavirDateInput")
const mavirMsgDiv = document.getElementById("mavirMsgDiv")
const mavirPlotDivId = "mavirPlotDiv"

// global variables
let mavirMinDate = null
let mavirMaxDate = null
let mavirRequestedMinDate = null
let mavirRequestedMaxDate = null
let mavirMeta = null
let mavirData = null
let mavirViewRange = 5
let mavirLastUpdate = null

// plot format
const mavirPlotFormat = {
    // net load
    NetSystemLoad: {
        name: 'Nettó rendszertehelés',
        color: 'rgb(51, 34, 136)',
        dash: 'solid'
    },
    NetSystemLoadFactPlantManagment: {
        name: 'Nettó tény rendszertehelés - üzemirányítási',
        color: 'rgb(136, 204, 238)',
        dash: 'solid'
    },
    NetSystemLoadNetTradeSettlement: {
        name: 'Nettó tény rendszertehelés - net.ker.elsz.meres',
        color: 'rgb(68, 170, 153)',
        dash: 'solid'
    },
    NetPlanSystemLoad: {
        name: 'Nettó terv rendszertehelés',
        color: 'rgb(17, 119, 51)',
        dash: 'solid'
    },
    NetSystemLoadDayAheadEstimate: {
        name: 'Nettó rendszerterhelés becslés (dayahead)',
        color: 'rgb(153, 153, 51)',
        dash: 'solid'
    },
    // production
    NetPlanSystemProduction: {
        name: 'Nettó terv rendszertermelés',
        color: 'rgb(0, 0, 0)',
        dash: 'dashdot'
    },
    // gross load
    GrossSystemLoad: {
        name: 'Bruttó rendszertehelés',
        color: 'rgb(204, 102, 119)',
        dash: 'dash'
    },
    GrossCertifiedSystemLoad: {
        name: 'Bruttó hitelesített rendszertehelés',
        color: 'rgb(136, 34, 85)',
        dash: 'dash'
    },
    GrossPlanSystemLoad: {
        name: 'Bruttó terv rendszertehelés',
        color: 'rgb(170, 68, 153)',
        dash: 'dash'
    },
    GrossSystemLoadDayAheadEstimate: {
        name: 'Bruttó rendszertehelés becslés (dayahead)',
        color: 'rgb(221, 204, 119)',
        dash: 'dash'
    },
}

// functions
function setMavirNavDisabled(disabled) {
    mavirForwardButton.disabled = disabled
    mavirBackwardButton.disabled = disabled
}

async function updateMavirMeta() {
    let meta = await fetchData(apiUrl + 'mavir/meta')
    mavirMeta = meta
}

function makeMavirLines(from, to) {
    // update mavir lineplot with given range, expects: from < to
    mavirMsgDiv.innerHTML = "<p>" +
        mavirData.Message.replace('MAVIR, source: (', '<a href=').replace(')', '>MAVIR</a>') +
        "</p>"
    let data = mavirData.data
    let x = []
    let ys = {}

    for (let key in mavirPlotFormat) {
        ys[key] = []
    }

    for (let i = from; i <= to; i = addMinutesToISODate(i, 10)) {
        let key = i.replace('T', ' ')
        let item = data[key]

        // display date in local time
        let date = new Date(key)
        date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
        x.push(localToUtcString(date).replace('T', ' '))

        for (let fet in mavirPlotFormat) {
            ys[fet].push(item[fet])
        }
    }

    let plotData = []

    for (let fet in mavirPlotFormat) {
        format = mavirPlotFormat[fet]
        plotData.push({
            type: 'scatter',
            x: x,
            y: ys[fet],
            mode: 'lines',
            name: format.name,
            line: {
                dash: format.dash,
                color: format.color,
                width: 2
            }
        })
    }

    let plotLayout = {
        font: {
            size: 12,
            color: 'rgb(255,255,255)'
        },
        autosize: true,
        margin: {
            l: 60,
            r: 10,
            b: 30,
            t: 20,
        },
        xaxis: {
            gridcolor: 'rgb(255,255,255)',
        },
        yaxis: {
            gridcolor: 'rgb(255,255,255)',
            ticksuffix: ' MW',
            hoverformat: '.1f'
        },
        showlegend: true,
        legend: {
            orientation: 'h'
        },
        height: 600,
        paper_bgcolor: 'rgba(75, 75, 75, 1)',
        plot_bgcolor: 'rgba(0, 0, 0, 0)',
        hoverlabel: {
            namelength: -1
        }
    }

    let plotConfig = {
        responsive: true,
        modeBarButtonsToRemove: [
            'pan2d',
            'zoom2d',
            'zoomIn2d',
            'zoomOut2d',
            'autoScale2d',
            'resetScale2d'
        ]
    }

    Plotly.newPlot(mavirPlotDivId, plotData, plotLayout, plotConfig)
}

async function updateMavirLines(datetime) {
    // update elload centered on given datetime
    if (mavirMeta === null) {
        await updateMavirMeta()
    }

    let from = addHoursToISODate(datetime, -mavirViewRange)
    let to = addHoursToISODate(datetime, mavirViewRange)

    let reRequest = false
    if (mavirRequestedMinDate === null || mavirRequestedMaxDate === null) {
        // setting a smaller range to reduce traffic
        mavirRequestedMinDate = addHoursToISODate(datetime, -10)
        mavirRequestedMaxDate = addHoursToISODate(datetime, 10)

        reRequest = true
    } else if ((from < mavirRequestedMinDate) || (to > mavirRequestedMaxDate)) {
        mavirRequestedMinDate = addHoursToISODate(datetime, -24)
        mavirRequestedMaxDate = addHoursToISODate(datetime, 24)

        if (mavirRequestedMaxDate > mavirMaxDate) {
            mavirRequestedMaxDate = mavirMaxDate
        }

        reRequest = true
    }

    if (reRequest) {
        setMavirNavDisabled(true)

        mavirData = await fetchData(
            apiUrl + "mavir/load?start_date=" + mavirRequestedMinDate + "&end_date=" + mavirMaxDate
        )

        setMavirNavDisabled(false)
    }

    makeMavirLines(from, to)
}

function updateMavirPlot() {
    // update all plots with data from datetime-local input
    let rounded = floorTo10Min(mavirDateInput.value + ":00")
    if (!validDate(localToUtcString(rounded), mavirMinDate, mavirMaxDate)) {
        rounded = new Date(mavirMaxDate)
        rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    }

    // Return to local time to set the element, and then back to utc
    rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    mavirDateInput.value = localToUtcString(rounded)
    rounded.setHours(rounded.getHours() + rounded.getTimezoneOffset() / 60)

    let datetime = localToUtcString(rounded)

    updateMavirLines(datetime).then()
}

async function updateMavir() {
    // update elements
    let result = calcMinMaxDate(mavirMeta)
    mavirMinDate = result.minDate
    mavirMaxDate = result.maxDate
    // min has to be set in local time while minDate remains in UTC for comparisons
    let inMin = new Date(mavirMinDate)
    inMin.setHours(inMin.getHours() - 2 * inMin.getTimezoneOffset() / 60)
    mavirDateInput.min = localToUtcString(inMin)
    // max has to be set in local time while maxDate remains in UTC for comparisons
    let inMax = new Date(mavirMaxDate)
    inMax.setHours(inMax.getHours() - 2 * inMax.getTimezoneOffset() / 60)
    mavirDateInput.max = localToUtcString(inMax)
}

// construct elements
function setupMavir() {
    // setup function, assumes that meta is set
    updateMavir()
    mavirDateInput.value = mavirDateInput.max
    addMinutesToInputRounded10(mavirDateInput, -60 * 24)

    updateMavirPlot()

    mavirDateInput.addEventListener("change", updateMavirPlot)

    mavirForwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(mavirDateInput, 10)
        updateMavirPlot()
    })
    mavirBackwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(mavirDateInput, -10)
        updateMavirPlot()
    })
}
