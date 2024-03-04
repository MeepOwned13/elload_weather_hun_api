const mavirUpdateButton = document.getElementById("mavirUpdateButton")
const mavirForwardButton = document.getElementById("mavirForwardButton")
const mavirBackwardButton = document.getElementById("mavirBackwardButton")
const mavirDateInput = document.getElementById("mavirDateInput")
const mavirMsgDiv = document.getElementById("mavirMsgDiv")
const mavirPlotDivId = "mavirPlotDiv"

let mavirMinDate = "2022-01-01T00:00:00"
let mavirMaxDate = "2024-03-02T15:00:00"
let mavirMeta = null
let mavirLastUpdate = null

async function updateMavirMeta() {
    let meta = await fetchData(apiUrl + 'mavir/meta')
    mavirMeta = meta
}

function makeMavirLines(loadData, plotElementId, msgDiv) {
    // construct lineplot from elload data
    msgDiv.innerHTML = "<p>" + loadData.Message + "</p>"
    let data = loadData.data
    let x = []
    let y_load = []
    let y_plan = []

    for (let key in data) {
        let item = data[key]

        // display date in local time
        let date = new Date(key)
        date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
        x.push(localToUtcString(date).replace('T', ' '))

        y_load.push(item["NetSystemLoad"])
        y_plan.push(item["NetPlanSystemLoad"])
    }


    let plotLoad = {
        type: 'scatter',
        x: x,
        y: y_load,
        mode: 'lines',
        name: 'Load',
        line: {
            color: 'rgb(219, 64, 82)',
            width: 4
        }
    }

    let plotPred = {
        type: 'scatter',
        x: x,
        y: y_plan,
        mode: 'lines',
        name: 'Plan',
        line: {
            color: 'rgb(55, 128, 191)',
            width: 2
        }
    }

    let plotData = [plotLoad, plotPred]

    let plotLayout = {
        title: 'MAVIR data',
        font: {
            family: 'Droid Serif, serif',
            size: 12
        },
        titlefont: {
            size: 16
        },
        height: 600,
    }

    let plotConfig = {
        responsive: true
    }

    Plotly.newPlot(plotElementId, plotData, plotLayout, plotConfig)
}

async function updateMavirLines(datetime) {
    // update elload on given datetime
    let date = new Date(datetime)

    let start = new Date(date.getTime())
    start.setHours(start.getHours() - 5)
    start_date = localToUtcString(start)

    let end = new Date(date.getTime())
    end.setHours(end.getHours() + 5)
    end_date = localToUtcString(end)

    const dataUrl = apiUrl + "mavir/load?col=netsystemload&col=netplansystemload&start_date=" +
        start_date + "&end_date=" + end_date

    const data = await fetchData(dataUrl)

    makeMavirLines(data, mavirPlotDivId, mavirMsgDiv)
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

    mavirUpdateButton.addEventListener("click", updateMavirPlot)
    mavirForwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(mavirDateInput, 10)
        updateMavirPlot()
    })
    mavirBackwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(mavirDateInput, -10)
        updateMavirPlot()
    })
}
