const omszMsgDiv = document.getElementById("omszMsgDiv")
const omszMapDivId = "omszStationMapDiv"
const omszUpdateButton = document.getElementById("omszUpdateButton")
const omszForwardButton = document.getElementById("omszForwardButton")
const omszBackwardButton = document.getElementById("omszBackwardButton")
const omszDateInput = document.getElementById("omszDateInput")

let omszMinDate = "2018-01-01T00:00:00"
let omszMaxDate = "2024-03-02T15:00:00"
const gradientStops = [
    [0, 212, 255],
    [254, 255, 0],
    [255, 128, 0],
    [255, 0, 0],
]
let omszMeta = null
let omszLastUpdate = null

async function updateOmszMeta() {
    let meta = await fetchData(apiUrl + 'omsz/meta')
    omszMeta = meta
}

function makeMap(stationMeta, stationData, datetime, plotElementId, msgDiv) {
    // Construct the stationMap
    msgDiv.innerHTML = "<p>" + stationMeta.Message + "</p>"
    // TODO: Make sure to display message from OMSZ
    stationMeta = stationMeta.data
    stationData = stationData.data
    let texts = []
    let lons = []
    let lats = []
    let colors = []
    let gradientColors = []
    let customData = []

    for (let key in stationMeta) {
        let item = stationMeta[key]
        let station = stationData[key]

        let value = null
        // station may be not retrieved, not have respective column or not have data for given time
        if (!(station === undefined) &&
            datetime in station &&
            !((value = station[datetime].Temp) === null)
        ) {
            let color = linearGradient(gradientStops, getPercentageInRange(0, 30, value))
            colors.push(arrToRGBA(color, 0))
            gradientColors.push(arrToRGBA(color, 1))
        } else {
            continue
        }

        texts.push(value.toString() + '°C ' + item.StationName)
        customData.push(value)
        lons.push(item.Longitude)
        lats.push(item.Latitude)
    }

    let data = [{
        type: 'scattergeo',
        mode: 'markers',
        text: texts,
        lon: lons,
        lat: lats,
        hoverinfo: "text",
        marker: {
            size: 50,
            color: colors,
            gradient: {
                color: gradientColors,
                type: "radial"
            }
        },
        textposition: [
            'top right', 'top left'
        ],
    }];
    let layout = {
        title: 'OMSZ stations',
        font: {
            family: 'Droid Serif, serif',
            size: 6
        },
        titlefont: {
            size: 16
        },
        geo: {
            scope: 'europe',
            resolution: 50,
            projection: {
                type: 'mercator'
            },
            lonaxis: {
                'range': [15.5, 23.5]
            },
            lataxis: {
                'range': [45.5, 49]
            },
            showrivers: true,
            rivercolor: '#00f',
            showlakes: true,
            lakecolor: '#55f',
            showland: true,
            showcountries: true,
            landcolor: '#121411',
            countrycolor: '#d3d3d3',
            countrywidth: 1,
            subunitcolor: '#a1a1a1'
        },
        autosize: true,
        margin: {
            l: 10,
            r: 10,
            b: 10,
            t: 40,
        },
        height: 600
    };
    let config = {
        responsive: true
    }

    Plotly.newPlot(plotElementId, data, layout, config);
}

async function updateMap(datetime) {
    // update of map on given datetime
    const dataUrl = apiUrl + 'omsz/weather?start_date=' + datetime + '&end_date=' + datetime + '&col=temp'

    if (omszMeta === null) {
        await updateOmszMeta()
    }

    let stationData = await fetchData(dataUrl)

    makeMap(omszMeta, stationData, datetime.replace('T', ' '), omszMapDivId, omszMsgDiv)
}

function updateOmszPlot() {
    // update all plots with data from datetime-local input
    let rounded = floorTo10Min(omszDateInput.value + ":00")
    if (!validDate(localToUtcString(rounded), omszMinDate, omszMaxDate)) {
        rounded = new Date(omszMaxDate)
        rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    }

    // Return to local time to set the element, and then back to utc
    rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    omszDateInput.value = localToUtcString(rounded)
    rounded.setHours(rounded.getHours() + rounded.getTimezoneOffset() / 60)

    let datetime = localToUtcString(rounded)

    updateMap(datetime).then()
}

async function updateOmsz() {
    // update elements
    let result = calcMinMaxDate(omszMeta)
    omszMinDate = result.minDate
    omszMaxDate = result.maxDate
    // min has to be set in local time while minDate remains in UTC for comparisons
    let inMin = new Date(omszMinDate)
    inMin.setHours(inMin.getHours() - 2 * inMin.getTimezoneOffset() / 60)
    omszDateInput.min = localToUtcString(inMin)
    // max has to be set in local time while maxDate remains in UTC for comparisons
    let inMax = new Date(omszMaxDate)
    inMax.setHours(inMax.getHours() - 2 * inMax.getTimezoneOffset() / 60)
    omszDateInput.max = localToUtcString(inMax)
}

// construct elements
function setupOmsz() {
    omszDateInput.value = omszDateInput.max
    updateOmszPlot()
    omszUpdateButton.addEventListener("click", updateOmszPlot)
    omszForwardButton.addEventListener("click", () => {
        let rounded = floorTo10Min(omszDateInput.value + ":00")
        rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
        rounded.setMinutes(rounded.getMinutes() + 10)
        omszDateInput.value = localToUtcString(rounded)
        updateOmszPlot()
    })
    omszBackwardButton.addEventListener("click", () => {
        let rounded = floorTo10Min(omszDateInput.value + ":00")
        rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
        rounded.setMinutes(rounded.getMinutes() - 10)
        omszDateInput.value = localToUtcString(rounded)
        updateOmszPlot()
    })
}
