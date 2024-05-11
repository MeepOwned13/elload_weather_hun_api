/**
* Abstract base class for classes making, updating plots while downloading data.
* The following methods should be implemented by child classes:
* display(), should be called when plot appears or reappears on screen
* async setup(index), should be called on pageload, takes an index that has a member on lastUpdateKey with last update time
* updatePlot(), should update the plot by taking the date from the _dateInput
*/
class PlotController {
    _dateInput
    _forwardButton
    _backwardButton
    _plotDiv
    _loadingOverlay
    _apiUrl
    _lastUpdateKey
    _maxWidth
    _stepSize
    _containerDiv
    _inputDiv

    _minDate = null
    _maxDate = null
    _status = null
    _lastUpdate = null

    /**
    * Initialize the controller, adds plotDiv, date input, forward and back buttons, loading overlay to container (containerId)
    * @param {String} apiUrl - url to api, should specify sub-path e.g. "{apiUrl}/omsz/"
    * @param {String} containerId - id of container to add elements to
    * @param {String} lastUpdateKey - key of update time in index given to setup(index)
    * @param {number} stepSize - stepSize for navigational buttons in minutes
    * @param {number} maxWidth - CSS dependant maximal size of containers inside (excludes padding)
    */
    constructor(apiUrl, containerId, lastUpdateKey, stepSize = 10, maxWidth = 1080) {
        // simulating abstract class/method, doesn't limit arguments
        // all arguments on the following functions should be optional, have default values
        if (this.display === undefined) {
            throw new TypeError("Must implement display method")
        }

        if (this.setup === undefined) {
            throw new TypeError("Must implement setup method")
        }

        if (this.updatePlot === undefined) {
            throw new TypeError("Must implement updatePlot method")
        }

        // rest of the constructor
        this._apiUrl = apiUrl
        this._lastUpdateKey = lastUpdateKey
        this._stepSize = stepSize
        this._maxWidth = maxWidth

        this._containerDiv = document.getElementById(containerId)

        this._plotDiv = document.createElement("div")
        this._containerDiv.appendChild(this._plotDiv)

        this._inputDiv = document.createElement("div")
        this._inputDiv.classList.add("inputs")
        this._containerDiv.appendChild(this._inputDiv)

        this._dateInput = document.createElement("input")
        this._dateInput.type = "datetime-local"
        this._dateInput.step = toString(this._stepSize * 60)
        this._inputDiv.appendChild(this._dateInput)

        let buttonDiv = document.createElement("div")

        this._backwardButton = document.createElement("button")
        this._backwardButton.innerHTML = "<i class=\"fa-solid fa-backward\"></i>"
        buttonDiv.appendChild(this._backwardButton)

        this._forwardButton = document.createElement("button")
        this._forwardButton.innerHTML = "<i class=\"fa-solid fa-forward\"></i>"
        buttonDiv.appendChild(this._forwardButton)

        this._inputDiv.appendChild(buttonDiv)

        this._loadingOverlay = document.createElement("div")
        this._loadingOverlay.classList.add("loading")
        this._loadingOverlay.innerHTML = "<div class=\"spinner\"></div>"
        this._containerDiv.appendChild(this._loadingOverlay)


    }

    /**
    * Sets navigational elements' disabled property, should be overloaded if new ones are added in sublcass 
    * If disabled=True then sets loading overlay, if disabled=False disabled loading overlay
    * @param {boolean} disabled - what disabled state to set
    */
    _setNavDisabled(disabled) {
        this._forwardButton.disabled = disabled
        this._backwardButton.disabled = disabled
        this._dateInput.disabled = disabled
        this._loadingOverlay.className = disabled ? "loading" : ""
    }

    /**
    * Updates status if necessary, decides based on given index (with lastUpdateKey) and internal state
    * @async
    * @param {Object} index - JSON return of index page containing last update time under lastUpdateKey
    * @returns {Promise<Boolean>} did an update happen?
    */
    async updateStatus(index) {
        if (this._lastUpdate === index[this._lastUpdateKey]) {
            return false
        }
        this._lastUpdate = index[this._lastUpdateKey]
        this._status = await fetchData(this._apiUrl + "status")
        this.updateDateInput()
        return true
    }

    /**
    * Update min and max date of datetime-local input based on saved status
    */
    updateDateInput() {
        let result = calcMinMaxDate(this._status)
        this._minDate = result.minDate
        this._maxDate = result.maxDate
        // min has to be set in local time while minDate remains in UTC for comparisons
        let inMin = new Date(this._minDate)
        inMin.setHours(inMin.getHours() - 2 * inMin.getTimezoneOffset() / 60)
        this._dateInput.min = localToUtcString(inMin)
        // max has to be set in local time while maxDate remains in UTC for comparisons
        let inMax = new Date(this._maxDate)
        inMax.setHours(inMax.getHours() - 2 * inMax.getTimezoneOffset() / 60)
        this._dateInput.max = localToUtcString(inMax)
    }
}

/**
* Abstract base class for classes making, updating lineplots plots while downloading data.
* The following methods should be implemented by child classes:
* setup(index), should be called on pageload, takes an index that has a member on lastUpdateKey with last update time
*/
class LinePlotController extends PlotController {
    _dataReqName
    _maxViewRange
    _minViewRange
    _plotFormat
    _viewRange

    _requestedMinDate = null
    _requestedMaxDate = null
    _data = null
    _resizeTimeout = null
    _showLegend = true

    /**
    * Supreclass constructor: Initialize the controller, adds plotDiv, date input, forward and back buttons, loading overlay to container (containerId)
    * Constructor: superclass + sets formatting and responsive view range
    * @param {String} apiUrl - url to api, should specify sub-path e.g. "{api}/omsz/"
    * @param {String} containerId - id of container to add elements to
    * @param {String} lastUpdateKey - key of update time in index given to setup(index)
    * @param {String} dataReqName - data request name added after apiUrl, data comes from "{apiUrl}/{dataReqName}", allows base parameter definitions via ?name=val&...
    * @param {number} maxViewRange - int specifing max range to display, goes negative and positive (-> double is displayed)
    * @param {number} minViewRange - int specifing the min range the responsive layout should display
    * @param {Object} plotFormat - object specifying col names from api as keys and objects as values that set -> name, color, dash
    * @param {number} stepSize - stepSize for navigational buttons in minutes
    * @param {number} maxWidth - CSS dependant maximal size of containers inside (excludes padding)
    */
    constructor(apiUrl, containerId, lastUpdateKey, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, containerId, lastUpdateKey, stepSize, maxWidth)
        this._dataReqName = dataReqName
        this._maxViewRange = maxViewRange
        this._viewRange = this._maxViewRange
        this._minViewRange = minViewRange
        this._plotFormat = structuredClone(plotFormat)
    }

    /**
    * Updates plot dimensions to display responsively, also updates plot
    * @async
    */
    async updatePlotAndDimensions() {
        let width = window.getComputedStyle(this._plotDiv).getPropertyValue("width").slice(0, -2)
        if (width === "au") return; // means width was auto, it isn't displayed
        width = parseInt(width)
        const part = (width - 300) / (this._maxWidth - 300)
        this._viewRange = this._minViewRange + Math.round((this._maxViewRange - this._minViewRange) * part)
        await this.updatePlot()

        // legend slides into plot even after updatePlot, seems like an oversight in Plotly.react, this solves it
        if (this._plotDiv.layout !== undefined) Plotly.relayout(this._plotDiv, this._plotDiv.layout)
    }

    /**
    * Starts plot update taking the middle from datetime-local input, limits range given by input to the viewRange
    * @async
    * @param {boolean} force - download data no matter if we have it cached
    */
    async updatePlot(force = false) {
        // update all plots with data from datetime-local input
        let rounded = floorToMinutes(this._dateInput.value + ":00", this._stepSize)

        if (addMinutesToISODate(rounded, this._viewRange * 60) > this._maxDate) {
            rounded = addMinutesToISODate(this._maxDate, -this._viewRange * 60)
        }
        if (addMinutesToISODate(rounded, this._viewRange * 60) < this._minDate) {
            rounded = addMinutesToISODate(this._minDate, this._viewRange * 60)
        }

        this._dateInput.value = addMinutesToISODate(rounded, -getTZOffset(rounded))

        await this._updateLines(rounded, force)
    }

    /**
    * Update/create line plot with given start and end
    * @param {String} from - ISO date to start at, must be less than "to" to work properly
    * @param {String} to - ISO date to end on, must be higher than "from" to work properly
    */
    _makeLines(from, to) {
        let data = this._data.data
        let x = []
        let ys = {}

        for (let key in this._plotFormat) {
            ys[key] = []
        }

        for (let i = from; i <= to; i = addMinutesToISODate(i, this._stepSize)) {
            let item = data[i]

            // display date in local time
            let date = new Date(i)
            date.setHours(date.getHours() - 2 * date.getTimezoneOffset() / 60)
            x.push(localToUtcString(date).replace("T", " "))
            for (let fet in this._plotFormat) {
                ys[fet].push(item[fet])
            }

        }

        let plotData = []

        let i = 0
        for (let fet in this._plotFormat) {
            let format = this._plotFormat[fet]
            plotData.push({
                type: "scatter",
                x: x,
                y: ys[fet],
                mode: "lines",
                name: format.name,
                text: format.name,
                line: {
                    dash: format.dash,
                    color: format.color,
                    width: 3
                },
                visible: this._plotDiv.data ? this._plotDiv.data[i++].visible : "true"
            })
        }

        const narrow = this._viewRange < this._minViewRange + Math.ceil((this._maxViewRange - this._minViewRange) * 0.3)

        let plotLayout = {
            font: {
                size: 16,
                color: "rgb(200, 200, 200)"
            },
            autosize: true,
            margin: {
                l: narrow ? 20 : 72,
                r: narrow ? 5 : 20,
                t: narrow ? 10 : 20,
            },
            xaxis: {
                gridcolor: "rgb(200, 200, 200)",
                nticks: narrow ? 3 : 6,
                fixedrange: true,
            },
            yaxis: {
                gridcolor: "rgb(200, 200, 200)",
                ticksuffix: " MW",
                tickangle: narrow ? -90 : 0,
                hoverformat: ".1f",
                nticks: narrow ? 4 : 6,
                fixedrange: true,
            },
            showlegend: this._showLegend,
            legend: {
                orientation: "h",
                xanchor: "center",
                x: 0.5,
                font: {
                    size: narrow ? 14 : 16,
                },
            },
            height: 672,
            paper_bgcolor: "rgba(0, 0, 0, 0)",
            plot_bgcolor: "rgba(0, 0, 0, 0)",
            hoverinfo: "text+x",
            hoverlabel: {
                font: {
                    size: 18,
                },
                namelength: 0,
            }
        }

        let plotConfig = {
            responsive: true,
            modeBarButtonsToRemove: [
                "pan2d",
                "zoom2d",
                "zoomIn2d",
                "zoomOut2d",
                "autoScale2d"
            ]
        }

        Plotly.react(this._plotDiv, plotData, plotLayout, plotConfig)
    }

    /**
    * Updates linePlot while downloading necessary data if required
    * Downloads more than needed to improve UI responsiveness
    * @async
    * @param {boolean} force - force download even if we have the data cached
    * @param {String} datetime - ISO string specifying the middle of viewed dates
    */
    async _updateLines(datetime, force = false) {
        let from = addMinutesToISODate(datetime, -this._viewRange * 60)
        let to = addMinutesToISODate(datetime, this._viewRange * 60)

        let reRequest = false
        if (this._requestedMinDate === null || this._requestedMaxDate === null) {
            // setting a smaller range to reduce load times
            this._requestedMinDate = addMinutesToISODate(datetime, -this._viewRange * 2 * 60)
            this._requestedMaxDate = addMinutesToISODate(datetime, this._viewRange * 2 * 60)

            reRequest = true
        } else if (force || (from < this._requestedMinDate) || (to > this._requestedMaxDate)) {
            this._requestedMinDate = addMinutesToISODate(datetime, -this._viewRange * 4 * 60)
            this._requestedMaxDate = addMinutesToISODate(datetime, this._viewRange * 4 * 60)

            if (this._requestedMaxDate > this._maxDate) {
                this._requestedMaxDate = this._maxDate
            }

            reRequest = true
        }

        if (reRequest) {
            this._setNavDisabled(true)

            let paramStartChar = this._dataReqName.includes("?") ? "&" : "?"
            this._data = await fetchData(
                this._apiUrl + this._dataReqName + paramStartChar +
                "start_date=" + this._requestedMinDate + "&end_date=" + this._requestedMaxDate
            )

            this._setNavDisabled(false)
        }

        this._makeLines(from, to)
    }

    /**
    * Updates plot with responsive layout, should be called on the appearing or reappearing of plot
    */
    display() {
        this.updatePlotAndDimensions().then()
    }
}

/**
* Class wrapping multiple PlotControllers to call common functions together 
* Should be used for PlotControllers on the same page in a multipage application
* Any given controller can be freely retrieved from "controllers" via their name
*/
class PageController {
    button
    #div
    controllers = {}

    /**
    * Sets div of page containing the PlotController divs
    * @param {String} buttonId - id of button to store (generally the switch button)
    * @param {String} divId - id of div to consider a page
    */
    constructor(buttonId, divId) {
        this.button = document.getElementById(buttonId)
        this.#div = document.getElementById(divId)
    }

    /**
    * Add a new controller to the page, doesn't initialize it!
    * @param {String} name - name to store PlotController under, useful for accessing later
    * @param {PlotController} ctl - PlotController to manage
    */
    addController(name, ctl) {
        this.controllers[name] = ctl
    }

    /**
    * Call setup for all controllers with given index
    * @async
    * @param {Object} index - index to use for setup
    */
    async setupControllers(index) {
        for (let key in this.controllers) {
            await this.controllers[key].setup(index)
        }
    }

    /**
    * Call status update for all controllers with given index
    * @async
    * @param {Object} index - index to use for update
    * @returns {Promise<Array>} names of updated controllers
    */
    async updateControllers(index) {
        // update Controllers with given index that contains lastUpdateKeys and lastUpdates
        // returns the names of updated controllers
        let updated = []
        for (let key in this.controllers) {
            if (await this.controllers[key].updateStatus(index)) {
                updated.push(key)
            }
        }
        return updated
    }

    /**
    * Sets page divs display to "none"
    */
    switchAway() {
        this.#div.style.display = "none"
    }

    /**
    * Runs display for all controllers and sets divs display to "block"
    */
    switchTo() {
        this.#div.style.display = "block"
        for (let key in this.controllers) {
            this.controllers[key].display()
        }
    }
}
