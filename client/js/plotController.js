class PlotController {
    _dateInput
    _forwardButton
    _backwardButton
    _loadingOverlay
    _apiUrl

    _minDate = null
    _maxDate = null
    _status = null

    constructor(apiUrl, dateInputId, forwardButtonId, backwardButtonId, loadingOverlayId) {
        this._apiUrl = apiUrl
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

    async updateStatus() {
        this._status = await fetchData(this._apiUrl + "status")
        this.updateDateInput()
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
