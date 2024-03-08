// global constants
const omszMsgDiv = document.getElementById("omszMsgDiv")
const omszMapDivId = "omszStationMapDiv"
const omszUpdateButton = document.getElementById("omszUpdateButton")
const omszForwardButton = document.getElementById("omszForwardButton")
const omszBackwardButton = document.getElementById("omszBackwardButton")
const omszDateInput = document.getElementById("omszDateInput")
const omszDropdown = document.getElementById("omszDropdown")

// globals variables
let omszMinDate = null
let omszMaxDate = null
let omszRequestedMinDate = null
let omszRequestedMaxDate = null
let omszMeta = null
let omszData = null
let omszLastUpdate = null

// map formatting Object
const omszMapFormat = {
    Temp: {
        name: 'Temperature',
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
        name: 'Average Temperature',
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
    MinTemp: {
        name: 'Minimum Temperature',
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
    MaxTemp: {
        name: 'Maximum Temperature',
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
        name: 'Precipitation',
        min: 0,
        max: 0.3,
        gradient: [
            [255, 255, 255, 0.05],
            [28, 189, 227, 1],
            [24, 0, 255, 1]
        ],
        measurement: 'mm'
    },
    AvgWS: {
        name: 'Average Wind',
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

// functions
function setNavButtonsDisabled(disabled) {
    omszForwardButton.disabled = disabled
    omszBackwardButton.disabled = disabled
}

async function updateOmszMeta() {
    let meta = await fetchData(apiUrl + 'omsz/meta')
    omszMeta = meta
}

function makeOmszMap(datetime, column) {
    // Construct the stationMap
    omszMsgDiv.innerHTML = "<p>" +
        omszMeta.Message.replace('OMSZ, source: (', '<a href=').replace(')', '>OMSZ</a>') +
        "</p>"

    let meta = omszMeta.data
    let data = omszData.data
    let format = omszMapFormat[column]

    let plotData = []

    for (let key in meta) {
        let item = meta[key]
        let station = data[key]

        let gradientColor = null
        let color = null
        let value = null
        // station may be not retrieved, not have respective column or not have data for given time
        if (!(station === undefined) &&
            datetime in station &&
            !((value = station[datetime][column]) === null) &&
            !(value === undefined)
        ) {
            let interpol = linearGradient(format.gradient, getPercentageInRange(format.min, format.max, value))
            gradientColor = arrToRGBA(interpol) // in the middle
            color = arrToRGBA(interpol, 0) // rest
        } else {
            continue
        }

        let text = value.toString() + format.measurement + ' ' + item.StationName.trim()
        let lon = item.Longitude
        let lat = item.Latitude

        let angle = 0
        let symbol = "circle"
        let size = 50

        if (format.directionFeature) {
            if (value === 0) {
                continue // if windstrength is 0, then skip it
            }
            angle = station[datetime][format.directionFeature]
            symbol = "arrow-wide"
            size = 27
            color = gradientColor
        }

        plotData.push({
            type: 'scattergeo',
            mode: 'markers',
            text: [text],
            lon: [lon],
            lat: [lat],
            hoverinfo: "text",
            marker: {
                angle: angle,
                angleref: "up",
                symbol: symbol,
                size: size,
                color: color,
                gradient: {
                    color: gradientColor,
                    type: "radial"
                },
            },
            textposition: [
                'top right', 'top left'
            ],
        })
    }

    let plotLayout = {
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
        height: 600,
        showlegend: false,
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)'
    }

    let plotConfig = {
        responsive: true
    }

    Plotly.newPlot(omszMapDivId, plotData, plotLayout, plotConfig);
}

async function updateOmszMap(datetime, column) {
    // update of map on given datetime, requests data on it's own
    if (omszMeta === null) {
        await updateOmszMeta()
    }

    let reRequest = false
    if (omszRequestedMinDate === null || omszRequestedMaxDate === null) {
        omszRequestedMaxDate = datetime // first request is always current time
        // let's set it 1 hour back for the first time to reduce traffic
        omszRequestedMinDate = addHoursToISODate(datetime, -1)

        reRequest = true
    } else if (!validDate(datetime, omszRequestedMinDate, omszRequestedMaxDate)) {
        omszRequestedMinDate = addHoursToISODate(datetime, -3)
        omszRequestedMaxDate = addHoursToISODate(datetime, 3)
        if (omszRequestedMaxDate > omszMaxDate) {
            omszRequestedMaxDate = omszMaxDate
        }

        reRequest = true
    }

    if (reRequest) {
        setNavButtonsDisabled(true)

        let cols = []
        for(let key in omszMapFormat) {
            cols.push(key)
            if(omszMapFormat[key].directionFeature) {
                cols.push(omszMapFormat[key].directionFeature)
            }
        }

        omszData = await fetchData(
            apiUrl + 'omsz/weather?start_date=' + omszRequestedMinDate + '&end_date=' + omszRequestedMaxDate +
            '&col=' + cols.join('&col=')
        )

        setNavButtonsDisabled(false)
    }

    makeOmszMap(datetime.replace('T', ' '), column)
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

    let column = omszDropdown.value
    if (!(column in omszMapFormat)) {
        throw new Error("Selected option (" + column + ") unavailable")
    }

    updateOmszMap(datetime, column).then()
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
    // setup function, assumes that meta is set
    updateOmsz()
    omszDateInput.value = omszDateInput.max

    let dropdownOptions = []
    for (let key in omszMapFormat) {
        dropdownOptions.push(
            '<option value="' + key + '">' + omszMapFormat[key].name + '</option>'
        )
    }
    omszDropdown.innerHTML = dropdownOptions.join('\n')

    updateOmszPlot()

    omszUpdateButton.addEventListener("click", updateOmszPlot)
    omszForwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(omszDateInput, 10)
        updateOmszPlot()
    })
    omszBackwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(omszDateInput, -10)
        updateOmszPlot()
    })

}
