class PlotController {
    _dateInput
    _forwardButton
    _backwardButton
    _loadingOverlay
    _apiUrl
    _lastUpdateKey

    _minDate = null
    _maxDate = null
    _status = null
    _lastUpdate = null

    constructor(apiUrl, lastUpdateKey, dateInputId, forwardButtonId, backwardButtonId, loadingOverlayId) {
        this._apiUrl = apiUrl
        this._lastUpdateKey = lastUpdateKey
        this._dateInput = document.getElementById(dateInputId)
        this._forwardButton = document.getElementById(forwardButtonId)
        this._backwardButton = document.getElementById(backwardButtonId)
        this._loadingOverlay = document.getElementById(loadingOverlayId)

        // simulating abstract class/method
        if (this.display === undefined) {
            throw new TypeError("Must implement display method")
        }

        if (this.setup === undefined) {
            throw new TypeError("Must implement setup method")
        }

        if (this.updatePlot === undefined) {
            throw new TypeError("Must implement updatePlot method")
        }
    }

    // functions
    _setNavDisabled(disabled) {
        this._forwardButton.disabled = disabled
        this._backwardButton.disabled = disabled
        this._loadingOverlay.className = disabled ? "loading" : ""
    }

    async updateStatus(index) {
        // updates status if necessary, decided from index that contains lastUpdateKey and lastUpdate associated with it
        if (this._lastUpdate === index[this._lastUpdateKey]) {
            return false
        }
        this._lastUpdate = index[this._lastUpdateKey]
        this._status = await fetchData(this._apiUrl + "status")
        this.updateDateInput()
        return true
    }

    updateDateInput() {
        // update the allowed dates in the datetime-local input
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

class PageController {
    button
    #div
    controllers = {}

    constructor(buttonId, divId) {
        this.button = document.getElementById(buttonId)
        this.#div = document.getElementById(divId)
    }

    addController(name, ctl) {
        this.controllers[name] = ctl
    }

    async setupControllers(index) {
        for (let key in this.controllers) {
            await this.controllers[key].setup(index)
        }
    }

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

    switchAway() {
        this.#div.style.display = "none"
    }

    switchTo() {
        this.#div.style.display = "block"
        for (let key in this.controllers) {
            this.controllers[key].display()
        }
    }
}
