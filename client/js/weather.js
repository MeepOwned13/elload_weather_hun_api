class OmszController extends PlotController {
    // constants
    #urlA = document.getElementById("omszUrlA")
    #logoImg = document.getElementById("omszLogo")
    #dropdown = document.getElementById("omszDropdown")
    #mapBaseLonAxis = {
        "min": 15.7,
        "max": 23.3
    } // Longitude to fit Hungary map
    #mapHeight = 672 // adjusted for width of 1080 that is maximal in the css (1100 - 2 × 10)
    #mapFormat // formatting information for the map

    // variables
    #requestedMinDate = null
    #requestedMaxDate = null
    #data = null
    #mapLonAxis = [this.#mapBaseLonAxis.min, this.#mapBaseLonAxis.max]
    #resizeTimeout = null

    constructor(apiUrl, lastUpdateKey, plotDivId, dateInputId, forwardButtonId, backwardButtonId,
        loadingOverlayId, mapFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, lastUpdateKey, plotDivId, dateInputId, forwardButtonId, backwardButtonId,
            loadingOverlayId, stepSize, maxWidth)
        this.#mapFormat = structuredClone(mapFormat)
    }

    // functions
    _setNavDisabled(disabled) {
        super._setNavDisabled(disabled)
        this.#dropdown.disabled = disabled
    }

    #makeMap(datetime, column) {
        // Construct the stationMap
        this.#urlA.href = this.#data.Message.match(/\(([^)]+)\)/)[1]

        let status = this._status.data
        let data = this.#data.data[datetime]
        if (data === undefined) {
            throw new Error("No data for " + datetime)
        }
        let format = this.#mapFormat[column]

        let plotData = []

        for (let key in data) {
            let item = status[key]
            let station = data[key]

            let color = null
            let value = null
            // station may be not retrieved, not have respective column or not have data for given time
            // since I'm assigning a value inside the if statement, I'll need a solution with && (cause: lazy execution)
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
                    'range': this.#mapLonAxis
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
                landcolor: '#0e010d00',
                countrycolor: '#e8e4c9',
                countrywidth: 3,
                subunitcolor: '#a1a1a1',
                bgcolor: '#e8e4c900',
            },
            autosize: true,
            margin: {
                l: 0,
                r: 0,
                b: 0,
                t: 0,
            },
            height: this.#mapHeight,
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

        Plotly.react(this._plotDiv, plotData, plotLayout, plotConfig);
    }

    async #updateMap(datetime, column) {
        // update of map on given datetime, requests data on its own
        let reRequest = false
        if (this.#requestedMinDate === null || this.#requestedMaxDate === null) {
            this.#requestedMaxDate = datetime // first request is always current time
            // let's set it 1 hour back for the first time to reduce traffic
            this.#requestedMinDate = addMinutesToISODate(datetime, -1 * 60)

            reRequest = true
        } else if (datetime < this.#requestedMinDate || datetime > this.#requestedMaxDate) {
            this.#requestedMinDate = addMinutesToISODate(datetime, -3 * 60)
            this.#requestedMaxDate = addMinutesToISODate(datetime, 3 * 60)
            if (this.#requestedMaxDate > this._maxDate) {
                this.#requestedMaxDate = this._maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this._setNavDisabled(true)

            let cols = []
            for (let key in this.#mapFormat) {
                cols.push(key)
                if (this.#mapFormat[key].directionFeature) {
                    cols.push(this.#mapFormat[key].directionFeature)
                }
            }

            this.#data = await fetchData(
                this._apiUrl + 'weather?start_date=' + this.#requestedMinDate + '&end_date=' + this.#requestedMaxDate +
                '&date_first=True&col=' + cols.join('&col=')
            )

            this._setNavDisabled(false)
        }

        this.#makeMap(datetime, column)
    }

    updatePlot() {
        // update all plots with data from datetime-local input
        let rounded = floorToMinutes(this._dateInput.value + ":00", this._stepSize)
        if (rounded < this._minDate || rounded > this._maxDate) {
            rounded = this._maxDate
        }

        this._dateInput.value = addMinutesToISODate(rounded, -getTZOffset(rounded))

        let column = this.#dropdown.value
        if (!(column in this.#mapFormat)) {
            throw new Error("Selected option (" + column + ") unavailable")
        }

        this.#updateMap(rounded, column).then()
    }

    updateMapDimensions() {
        let width = window.getComputedStyle(this._plotDiv).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        width = parseInt(width)
        const part = width / this._maxWidth
        const newLotRange = (this.#mapBaseLonAxis.max - this.#mapBaseLonAxis.min) * part
        const centerLot = (this.#mapBaseLonAxis.max + this.#mapBaseLonAxis.min) / 2
        this.#mapLonAxis[0] = centerLot - newLotRange / 2
        this.#mapLonAxis[1] = centerLot + newLotRange / 2
    }

    // construct elements
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max

        let dropdownOptions = []
        for (let key in this.#mapFormat) {
            dropdownOptions.push(
                '<option value="' + key + '">' + this.#mapFormat[key].name + '</option>'
            )
        }
        this.#dropdown.innerHTML = dropdownOptions.join('\n')

        fetchData(this._apiUrl + 'logo').then((resp) => {
            this.#logoImg.src = resp
        })

        this.updateMapDimensions()
        this.updatePlot()

        this._dateInput.addEventListener("change", () => {
            this.updatePlot()
        })
        this.#dropdown.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, this._stepSize)
            this.updatePlot()
        }, 200, "omszForward")

        addIntervalToButton(this._backwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize)
            this.updatePlot()
        }, 200, "omszBackward")

        window.addEventListener('resize', () => {
            clearTimeout(this.#resizeTimeout)
            this.#resizeTimeout = setTimeout(() => {
                this.updateMapDimensions()
            }, 50)
        })
    }

    display() {
        this.updateMapDimensions()
        // redraw, screenwidth might have changed while on other page
        if (this._plotDiv.layout !== undefined) this.updatePlot()
    }
}
