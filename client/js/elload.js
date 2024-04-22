class MavirController extends LinePlotController {
    // constants
    #urlA
    #logoImg
    #legendCheckbox

    /**
    * Supreclass constructor: Initialize the controller, adds plotDiv, date input, forward and back buttons, loading overlay to container (containerId)
    * Constructor: superclass + sets formatting, selects url element, legend checkbox and responsive view range
    * @param {String} apiUrl - url to api, should specify sub-path e.g. "{api}/omsz/"
    * @param {String} containerId - id of container to add elements to
    * @param {String} lastUpdateKey - key of update time in index given to setup(index)
    * @param {String} urlAId - id of <a> element to put data source url into
    * @param {String} dataReqName - data request name added after apiUrl, data comes from "{apiUrl}/{dataReqName}", allows base parameter definitions via ?name=val&...
    * @param {number} maxViewRange - int specifing max range to display, goes negative and positive (=> double is displayed)
    * @param {number} minViewRange - int specifing the min range the responsive layout should display
    * @param {Object} plotFormat - object specifying col names from api as keys and objects as values that set => name, color, dash
    * @param {number} stepSize - stepSize for navigational buttons in minutes
    * @param {number} maxWidth - CSS dependant maximal size of containers inside (excludes padding)
    */
    constructor(apiUrl, containerId, lastUpdateKey, urlAId, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, containerId, lastUpdateKey, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize, maxWidth)

        this.#logoImg = document.createElement("img")
        this.#logoImg.classList.add("mavirLogo")
        this.#logoImg.src = ""
        this.#logoImg.alt = "Logo of MAVIR"
        this._containerDiv.insertBefore(this.#logoImg, this._containerDiv.firstChild)

        let legendDiv = document.createElement("div")

        let legendID = "showLegend" + UniqueID.next()
        this.#legendCheckbox = document.createElement("input")
        this.#legendCheckbox.type = "checkbox"
        this.#legendCheckbox.checked = true
        this.#legendCheckbox.id = legendID
        legendDiv.appendChild(this.#legendCheckbox)

        let legendLabel = document.createElement("label")
        legendLabel.htmlFor = legendID
        legendLabel.innerText = langStringText("showLegend")
        legendDiv.appendChild(legendLabel)

        this._inputDiv.appendChild(legendDiv)

        this.#urlA = document.getElementById(urlAId)
    }

    _makeLines(from, to) {
        super._makeLines(from, to)
        this.#urlA.href = this._data.Message.match(/\(([^)]+)\)/)[1]
    }

    /**
    * Sets up all elements of the controller, adds event listeners and display plot with max available dates visible
    * + Sets up checkbox to display legend or not
    * @async
    * @param {Object} index - JSON return of index page containing last update time under lastUpdateKey
    */
    async setup(index) {
        // index should contain lastUpdate times from API
        await this.updateStatus(index)
        this._dateInput.value = this._dateInput.max
        addMinutesToInputFloored(this._dateInput, this._stepSize, -60 * 24)

        fetchData(this._apiUrl + "logo").then((resp) => {
            this.#logoImg.src = resp
        })

        this.updatePlotAndDimensions() // this also calls updatePlot

        this._dateInput.addEventListener("change", () => {
            this.updatePlot()
        })

        addIntervalToButton(this._forwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, this._stepSize)
            this.updatePlot()
        }, 100, "mavirForward")

        addIntervalToButton(this._backwardButton, () => {
            addMinutesToInputFloored(this._dateInput, this._stepSize, -this._stepSize)
            this.updatePlot()
        }, 100, "mavirBackward")

        this.#legendCheckbox.checked = true
        this.#legendCheckbox.addEventListener("change", () => {
            this._showLegend = this.#legendCheckbox.checked
            this.updatePlot()
        })

        window.addEventListener("resize", () => {
            clearTimeout(this._resizeTimeout)
            this._resizeTimeout = setTimeout(() => {
                this.updatePlotAndDimensions()
            }, 50)
        })
    }
}
