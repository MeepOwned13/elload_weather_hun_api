class OmszController extends PlotController {
    // constants
    #urlA
    #logoImg
    #dropdown
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

    /**
    * Supreclass constructor: Initialize the controller, adds plotDiv, date input, forward and back buttons, loading overlay to container (containerId)
    * Constructor: superclass + sets formatting, creates dropdown for column select and selects url element
    * @param {String} apiUrl - url to api, should specify sub-path e.g. "{api}/omsz/"
    * @param {String} containerId - id of container to add elements to
    * @param {String} lastUpdateKey - key of update time in index given to setup(index)
    * @param {String} urlAId - id of <a> element to put data source url into
    * @param {Object} mapFormat - object specifying col names from api as keys and objects as values that set => name, min, max and gradient for colors, measurement and directionFeature if needed
    * @param {number} stepSize - stepSize for navigational buttons in minutes
    * @param {number} maxWidth - CSS dependant maximal size of containers inside (excludes padding)
    */
    constructor(apiUrl, containerId, lastUpdateKey, urlAId, mapFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, containerId, lastUpdateKey, stepSize, maxWidth)

        this.#logoImg = document.createElement("img")
        this.#logoImg.classList.add("omszLogo")
        this.#logoImg.src = ""
        this.#logoImg.alt = "Logo of OMSZ"
        this._containerDiv.insertBefore(this.#logoImg, this._containerDiv.firstChild)

        this._plotDiv.classList.add("omszStationMapDiv")
        let hider = document.createElement("div")
        hider.classList.add("hider")
        this._plotDiv.appendChild(hider)

        this.#dropdown = document.createElement("select")
        this._inputDiv.appendChild(this.#dropdown)

        this.#urlA = document.getElementById(urlAId)

        this.#mapFormat = structuredClone(mapFormat)
    }

    _setNavDisabled(disabled) {
        super._setNavDisabled(disabled)
        this.#dropdown.disabled = disabled
    }

    /**
    * Update/create map scatter plot at given time and display given feature/columns
    * @param {String} datetime - ISO date to display info on
    * @param {String} column - name of columns to display, specifies data and formatting given in mapFormat
    */
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

            let value = null
            // station may be not retrieved, not have respective column or not have data for given time
            // since I'm assigning a value inside the if statement, I'll need a solution with && (cause: lazy execution)
            if (((value = station[column]) === null) || (value === undefined)) {
                continue
            }
            let text = value.toString() + format.measurement + " " + item.StationName.trim()
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
                type: "scattergeo",
                mode: "markers",
                text: [text],
                lon: [lon],
                lat: [lat],
                hoverinfo: "text",
                marker: {
                    angle: angle,
                    angleref: "up",
                    symbol: symbol,
                    size: size,
                    color: [value], // needs to be in Array for coloraxis
                    coloraxis: 'coloraxis'
                },
                textposition: [
                    "top right", "top left"
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
                scope: "europe",
                resolution: 50,
                projection: {
                    type: "mercator"
                },
                lonaxis: {
                    "range": this.#mapLonAxis
                },
                lataxis: {
                    "range": [45.6, 48.8]
                },
                showrivers: true,
                rivercolor: "#0c1ba3",
                riverwidth: 4,
                showlakes: true,
                lakecolor: "#0c1ba3",
                showcountries: true,
                countrycolor: "#e8e4c9",
                countrywidth: 3,
                subunitcolor: "#a1a1a1",
                bgcolor: "#e8e4c900",
            },
            coloraxis: {
                cmin: format.min,
                cmax: format.max,
                colorscale: format.colorscale,
                showscale: true,
                colorbar: {
                    yref: 'paper',
                    yanchor: 'top',
                    y: 0.42,
                    xref: 'paper',
                    xanchor: 'right',
                    x: 1.0,
                    len: 0.4,
                    outlinecolor: "#0e010d",
                    nticks: 5,
                    ticksuffix: format.measurement + "        ", // there is no left alignment so this is the way
                    ticklabeloverflow: "allow", // to allow spaces for positioning above
                    ticklabelposition: "inside",
                    ticks: "inside",
                    tickwidth: 2,
                    ticklen: 8,
                    tickcolor: "#0e010d",
                    tickfont: {
                        color: "#e8e4c9",
                        size: 18,
                    },
                }
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
            paper_bgcolor: "rgba(0,0,0,0)",
            plot_bgcolor: "rgba(0,0,0,0)",
        }

        let plotConfig = {
            responsive: true,
            modeBarButtonsToRemove: [
                "select2d",
                "lasso2d"
            ]
        }

        Plotly.react(this._plotDiv, plotData, plotLayout, plotConfig);
    }

    /**
    * Updates map while downloading necessary data if required
    * Downloads more than needed to improve UI responsiveness
    * @async
    * @param {String} datetime - ISO string specifying the viewed date
    * @param {String} column - name of columns to display, specifies data and formatting given in mapFormat
    */
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
                this._apiUrl + "weather?start_date=" + this.#requestedMinDate + "&end_date=" + this.#requestedMaxDate +
                "&date_first=True&col=" + cols.join("&col=")
            )

            this._setNavDisabled(false)
        }

        this.#makeMap(datetime, column)
    }

    /**
    * Starts plot update taking the middle from datetime-local input, limits range given by input to ones available based on status
    * @async
    */
    async updatePlot() {
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

        await this.#updateMap(rounded, column)
    }


    /**
    * Updates viewed longitude to display responsively
    */
    updateMapDimensions() {
        let width = window.getComputedStyle(this._plotDiv).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        width = parseInt(width)
        const part = width / this._maxWidth
        const newLonRange = (this.#mapBaseLonAxis.max - this.#mapBaseLonAxis.min) * part
        const centerLot = (this.#mapBaseLonAxis.max + this.#mapBaseLonAxis.min) / 2
        this.#mapLonAxis[0] = centerLot - newLonRange / 2
        this.#mapLonAxis[1] = centerLot + newLonRange / 2
    }

    /**
    * Sets up all elements of the controller, adds event listeners and display plot with max available dates visible
    * + sets up dropdown to choose displayed feature/column
    * @async
    * @param {Object} index - JSON return of index page containing last update time under lastUpdateKey
    */
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max

        let dropdownOptions = []
        for (let key in this.#mapFormat) {
            dropdownOptions.push(
                "<option value=\"" + key + "\">" + this.#mapFormat[key].name + "</option>"
            )
        }
        this.#dropdown.innerHTML = dropdownOptions.join("\n")


        fetchData(this._apiUrl + "logo").then((resp) => {
            this.#logoImg.src = resp
        })

        this.updateMapDimensions()
        await this.updatePlot()

        // Panning shouldn't move page
        this._plotDiv.addEventListener("touchmove", (event) => {
            event.preventDefault()
        })

        this._dateInput.addEventListener("change", async () => {
            await this.updatePlot()
        })
        this.#dropdown.addEventListener("change", async () => {
            await this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, async () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, this._stepSize)
            await this.updatePlot()
        }, 200, "omszForward")

        addIntervalToButton(this._backwardButton, async () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize)
            await this.updatePlot()
        }, 200, "omszBackward")

        window.addEventListener("resize", () => {
            clearTimeout(this.#resizeTimeout)
            this.#resizeTimeout = setTimeout(() => {
                this.updateMapDimensions()
            }, 50)
        })
    }

    /**
    * Updates plot with responsive layout, should be called on the appearing or reappearing of plot
    */
    display() {
        this.updateMapDimensions()
        // redraw, screenwidth might have changed while on other page
        if (this._plotDiv.layout !== undefined) this.updatePlot().then()
    }
}
