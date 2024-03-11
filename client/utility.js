const apiUrl = 'https://8000-01hq0fcfq8q8tabmwrb7nb3x24.cloudspaces.litng.ai/'

function calcMinMaxDate(meta) {
    // calculate min and max based on meta, should work the same for omsz and mavir
    data = meta.data

    minDate = '9999999999999999999999999999' // placeholder
    maxDate = ''

    for (let key in data) {
        let item = data[key]
        minDate = minDate > item.StartDate ? item.StartDate : minDate
        maxDate = maxDate < item.EndDate ? item.EndDate : maxDate
    }

    minDate = minDate.replace(' ', 'T')
    maxDate = maxDate.replace(' ', 'T')

    return {
        'minDate': minDate,
        'maxDate': maxDate
    }
}

function validDate(date, minDate, maxDate) {
    // validate date based on minDate and maxDate
    return minDate <= date && date <= maxDate
}

function getPercentageInRange(min, max, value) {
    // get percentage position of value in range specified by min and max
    let length = max - min
    let position = value - min
    return position / length
}

function arrToRGBA(arr, alpha = null) {
    // transform 3 element array to rgba color with given alpha
    if (alpha === null || alpha === undefined) {
        alpha = arr[3]
    }
    return "rgba(" + arr[0] + "," + arr[1] + "," + arr[2] + "," + alpha + ")"
}

function linearGradient(stops, value) {
    // calculate linearGradient with value in [0,1] and given colorstops (even gradient)
    const stopLength = 1 / (stops.length - 1)
    const valueRatio = value / stopLength
    const stopIndex = Math.floor(valueRatio)
    if (stopIndex >= (stops.length - 1)) {
        return stops[stops.length - 1]
    } else if (stopIndex < 0) {
        return stops[0]
    }
    const stopFraction = valueRatio % 1
    return lerp(stops[stopIndex], stops[stopIndex + 1], stopFraction)
}

function lerp(pointA, pointB, normalValue) {
    return [
        pointA[0] + (pointB[0] - pointA[0]) * normalValue,
        pointA[1] + (pointB[1] - pointA[1]) * normalValue,
        pointA[2] + (pointB[2] - pointA[2]) * normalValue,
        pointA[3] + (pointB[3] - pointA[3]) * normalValue,
    ]
}

async function fetchData(url) {
    // async fetch data from given url and handle errors
    // May throw errors while fetching!
    const response = await fetch(url)

    if (!response.ok) {
        throw new Error("Couldn't fetch " + url)
    }

    const data = await response.json()

    return data
}

function floorTo10Min(timeString) {
    // floor given timestring to 10 min
    const datetime = new Date(timeString);

    const roundedMinutes = Math.floor(datetime.getMinutes() / 10) * 10;

    return new Date(
        datetime.getFullYear(),
        datetime.getMonth(),
        datetime.getDate(),
        datetime.getHours(),
        roundedMinutes,
        0
    );
}

function localToUtcString(localDate) {
    // convert given Date to utcstring HTML elements understand
    const utcDatetime = localDate.toISOString().replace(/.\d{3}Z$/, '') // Remove milliseconds and append 'Z'
    return utcDatetime
}

function addHoursToISODate(date, hours) {
    let datetime = new Date(date)
    // let's set it 1 hour back for the first time to reduce traffic
    datetime.setHours(datetime.getHours() + hours - datetime.getTimezoneOffset() / 60)
    return localToUtcString(datetime)
}

function addMinutesToISODate(date, minutes) {
    let datetime = new Date(date)
    // let's set it 1 hour back for the first time to reduce traffic
    datetime.setMinutes(datetime.getMinutes() + minutes - datetime.getTimezoneOffset())
    return localToUtcString(datetime)
}

function addMinutesToInputRounded10(dateInput, minutes) {
    // rounds input value to 10 Min before addition 
    let rounded = floorTo10Min(dateInput.value + ":00")
    rounded.setHours(rounded.getHours() - rounded.getTimezoneOffset() / 60)
    rounded.setMinutes(rounded.getMinutes() + minutes)
    dateInput.value = localToUtcString(rounded)

}
