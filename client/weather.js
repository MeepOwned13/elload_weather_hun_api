// global constants
const omszMsgDiv = document.getElementById("omszMsgDiv")
const omszMapDivId = "omszStationMapDiv"
const omszForwardButton = document.getElementById("omszForwardButton")
const omszBackwardButton = document.getElementById("omszBackwardButton")
const omszDateInput = document.getElementById("omszDateInput")
const omszDropdown = document.getElementById("omszDropdown")
const omszLogoImg = document.getElementById("omszLogo")
const omszMapBaseLotAxis = {
    "min": 15.7,
    "max": 23.3
} // Longitude to fit Hungary map
const omszMapBaseWidth = 1080 // maximal width defined via css
const omszMapHeight = 672 // adjusted for width of 1080 that is maximal in the css (1100 - 2*10)

// globals variables
let omszMinDate = null
let omszMaxDate = null
let omszRequestedMinDate = null
let omszRequestedMaxDate = null
let omszMeta = null
let omszData = null
let omszLastUpdate = null
let omszMapLotAxis = [omszMapBaseLotAxis.min, omszMapBaseLotAxis.max]
let omszResizeTimeout = null

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
function setOmszNavDisabled(disabled) {
    omszDropdown.disabled = disabled
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
    let data = omszData.data[datetime]
    if (data === undefined) {
        throw new Error("No data for " + datetime)
    }
    let format = omszMapFormat[column]

    let plotData = []

    for (let key in data) {
        let item = meta[key]
        let station = data[key]

        let color = null
        let value = null
        // station may be not retrieved, not have respective column or not have data for given time
        // since I'm assigning a value inside of the if statement, I'll need a solution with && (cause: lazy execution)
        if (((value = station[column]) === null) || (value === undefined)) {
            continue
        }
        let interpol = linearGradient(format.gradient, getPercentageInRange(format.min, format.max, value))
        color = arrToRGBA(interpol)

        let text = value.toString() + format.measurement + ' ' + item.StationName.trim()
        let lon = item.Longitude
        let lat = item.Latitude

        let angle = 0
        let symbol = "circle"
        let size = 25

        if (format.directionFeature) {
            if (value === 0) {
                continue // if windstrength is 0, then skip it
            }
            angle = station[format.directionFeature]
            symbol = "arrow-wide"
            size = 27
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
            },
            textposition: [
                'top right', 'top left'
            ],
            hoverlabel: {
                font: {
                    color: "rgb(0, 0, 0)",
                    size: 20,
                },
                namelength: -1
            }
        })
    }

    let plotLayout = {
        font: {
            size: 20
        },
        geo: {
            scope: 'europe',
            resolution: 50,
            projection: {
                type: 'mercator'
            },
            lonaxis: {
                'range': omszMapLotAxis
            },
            lataxis: {
                'range': [45.6, 48.8]
            },
            showrivers: true,
            rivercolor: '#0c1ba3',
            riverwidth: 4,
            showlakes: true,
            lakecolor: '#0c1ba3',
            showland: true,
            showcountries: true,
            landcolor: '#0e010d',
            countrycolor: '#e8e4c9',
            countrywidth: 3,
            subunitcolor: '#a1a1a1',
            bgcolor: '#e8e4c9',
        },
        autosize: true,
        margin: {
            l: 0,
            r: 0,
            b: 0,
            t: 0,
        },
        height: omszMapHeight,
        showlegend: false,
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
    }

    let plotConfig = {
        responsive: true,
        modeBarButtonsToRemove: [
            'select2d',
            'lasso2d'
        ]
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
        setOmszNavDisabled(true)

        let cols = []
        for (let key in omszMapFormat) {
            cols.push(key)
            if (omszMapFormat[key].directionFeature) {
                cols.push(omszMapFormat[key].directionFeature)
            }
        }

        omszData = await fetchData(
            apiUrl + 'omsz/weather?start_date=' + omszRequestedMinDate + '&end_date=' + omszRequestedMaxDate +
            '&date_first=True&col=' + cols.join('&col=')
        )

        setOmszNavDisabled(false)
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

function updateOmszMapDimensions() {
    const width = window.getComputedStyle(document.getElementById(omszMapDivId)).getPropertyValue("width").slice(0, -2)
    if (width == "au") return; // means width was auto, it isn't displayed
    const part = width / omszMapBaseWidth
    const newLotRange = (omszMapBaseLotAxis.max - omszMapBaseLotAxis.min) * part
    const centerLot = (omszMapBaseLotAxis.max + omszMapBaseLotAxis.min) / 2
    omszMapLotAxis[0] = centerLot - newLotRange / 2
    omszMapLotAxis[1] = centerLot + newLotRange / 2
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

    fetchData(apiUrl + 'omsz/logo').then((resp) => {
        omszLogoImg.src = resp
    })

    updateOmszMapDimensions()
    updateOmszPlot()

    omszDateInput.addEventListener("change", updateOmszPlot)
    omszDropdown.addEventListener("change", updateOmszPlot)

    omszForwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(omszDateInput, 10)
        updateOmszPlot()
    })
    omszBackwardButton.addEventListener("click", () => {
        addMinutesToInputRounded10(omszDateInput, -10)
        updateOmszPlot()
    })

    window.addEventListener('resize', function() {
        clearTimeout(omszResizeTimeout)
        omszResizeTimeout = this.setTimeout(updateOmszMapDimensions, 50)
    })
}