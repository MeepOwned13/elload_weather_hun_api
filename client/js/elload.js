class MavirController extends LinePlotController {
    // constants
    #urlA
    #logoImg
    #legendCheckbox

    constructor(apiUrl, containerId, lastUpdateKey, urlAId, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize = 10, maxWidth = 1080) {
        super(apiUrl, containerId, lastUpdateKey, dataReqName, maxViewRange, minViewRange, plotFormat, stepSize, maxWidth)

        this.#logoImg = document.createElement("img")
        this.#logoImg.classList.add("mavirLogo")
        this.#logoImg.src = ""
        this.#logoImg.alt = "Logo of MAVIR"
        this._containerDiv.insertBefore(this.#logoImg, this._containerDiv.firstChild)

        let legendID = "showLegend" + toString(UniqueID.next())
        this.#legendCheckbox = document.createElement("input")
        this.#legendCheckbox.type = "checkbox"
        this.#legendCheckbox.checked = true
        this.#legendCheckbox.id = legendID
        this._inputDiv.appendChild(this.#legendCheckbox)

        let legendLabel = document.createElement("label")
        legendLabel.htmlFor = legendID
        legendLabel.innerText = langStringText("showLegend")
        this._inputDiv.appendChild(legendLabel)

        this.#urlA = document.getElementById(urlAId)
    }

    _makeLines(from, to) {
        super._makeLines(from, to)
        this.#urlA.href = this._data.Message.match(/\(([^)]+)\)/)[1]
    }

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
