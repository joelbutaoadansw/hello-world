const sql = require('mssql')
const config = require('./config') // view config.js.txt
const axios = require('axios');
console.log('Start')
const sql_query = {
//    new_addresses:"SELECT top 50 dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch.EmergencyDentistSearchID, Practice + ' ' + ISNULL(Street,'') + ' ' + City + ' ' + State + ' ' + PostCode as SearchAddressFormat FROM dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch left outer join dbo.ADANSW_DEDGeoCodes on dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch.EmergencyDentistSearchID = dbo.ADANSW_DEDGeoCodes.EmergencyDentistSearchID where agree = 1 and dbo.ADANSW_DEDGeoCodes.SearchAddressFormat is not null and Practice + ' ' + ISNULL(Street,'') + ' ' + City + ' ' + State + ' ' + PostCode <> dbo.ADANSW_DEDGeoCodes.SearchAddressFormat",
    new_addresses:"SELECT top 5 dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch.EmergencyDentistSearchID, Practice + ' ' + ISNULL(Street,'') + ' ' + City + ' ' + State + ' ' + PostCode as SearchAddressFormat, Practice + ' ' + City + ' ' + State + ' ' + PostCode as SearchAddressFormatNoStreet FROM dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch left outer join dbo.ADANSW_DEDGeoCodes on dbo.Form_AustralianDentalAssociation_EmergencyDentistSearch.EmergencyDentistSearchID = dbo.ADANSW_DEDGeoCodes.EmergencyDentistSearchID where agree = 1 and dbo.ADANSW_DEDGeoCodes.SearchAddressFormat is not null and convert(nvarchar(max),HASHBYTES('MD5',Practice + ' ' + ISNULL(Street,'') + ' ' + City + ' ' + State + ' ' + PostCode),2) <> dbo.ADANSW_DEDGeoCodes.SearchAddressFormat",
}
const configKentico = {
    user: config.user,
    password: config.password,
    server: config.server,
    database: config.database,
    parseJSON: true,
    options: { enableArithAbort: false, }
}
sql.connect(configKentico).then(async () => {
    const request = new sql.Request();
    request.stream = true; // You can set streaming differently for each request
    request.query(sql_query.new_addresses);
    var i=0;

    request.on('row', async row => {
        var new_record = await getGeoCode(row,row.SearchAddressFormat);
        var aus = JSON.parse(new_record.GeoCode).address_components.find( ac => { return ac.long_name == 'Australia'; } );
        try {
            console.log(aus.long_name);
        } catch {
            console.log('Not Australia')
            new_record = await getGeoCode(row,row.SearchAddressFormatNoStreet);
        }
        //console.log(new_record);
        console.log(i++);
        await updateNewRecord(new_record);
    })

    request.on('error', err => {
        console.log('Error on select request:',err)
    })

})
.then(result => {
    console.log('Complete sql.connect.select')
}).catch(err => {
    console.log('Error on sql.connect.select',err)
})

getGeoCode = async (row,saf) => {
    let new_record;
    let url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(saf)}&key=${config.api_key}`;
    try {
        r = await axios.get(url);
        if (r.status == 200 && r.data.results.length > 0) {
            let data = r.data.results[0];
            new_record = {
                EmergencyDentistSearchID: row.EmergencyDentistSearchID,
                SearchAddressFormat: row.SearchAddressFormat,
                GeoCode: JSON.stringify(data),
                Latitude: data.geometry.location.lat,
                Longitude: data.geometry.location.lng,
                PlaceID:data.place_id,
                FormattedAddress:data.formatted_address,
            }
            //await insertNewRecord(new_record);
        } else {
            new_record = {
                EmergencyDentistSearchID: row.EmergencyDentistSearchID,
                SearchAddressFormat: row.SearchAddressFormat,
                GeoCode: 'BLANK',
                Latitude: '0.0',
                Longitude: '0.0',
                PlaceID:'',
                FormattedAddress:row.SearchAddressFormat,
            }
            //await insertNewRecord(new_record);
            console.log('Error','No result returned on Google GeoCode request');
        }
    } catch (error) {
        console.log('Error on Google GeoCode request:',error, 'Record:', row.EmergencyDentistSearchID);
        r = '';
    }
    return new_record;
}

updateNewRecord = async (newrecord) => {
    let pool = await sql.connect(configKentico);
    const request = await pool.request();
    request.input('EmergencyDentistSearchID', sql.Int, newrecord.EmergencyDentistSearchID)
    request.input('SearchAddressFormat', sql.VarChar, newrecord.SearchAddressFormat)
    request.input('GeoCode', sql.VarChar, newrecord.GeoCode)
    request.input('Latitude', sql.VarChar, newrecord.Latitude)
    request.input('Longitude', sql.VarChar, newrecord.Longitude)
    request.input('FormattedAddress', sql.VarChar, newrecord.FormattedAddress)
    request.input('PlaceID', sql.VarChar, newrecord.PlaceID)
    request.query("UPDATE dbo.ADANSW_DEDGeoCodes SET GeoCode = @GeoCode,Latitude = @Latitude,Longitude = @Longitude,FormattedAddress = @FormattedAddress,DateCreated = getdate(),PlaceID = @PlaceID,SearchAddressFormat = convert(nvarchar(max),HASHBYTES('MD5',@SearchAddressFormat),2) WHERE EmergencyDentistSearchID = @EmergencyDentistSearchID", 
        (err, result) => {
        console.log("Updated:",newrecord.EmergencyDentistSearchID, `(${newrecord.PlaceID})`)
    })
}

sql.on('error', err => {
    console.log('Top Level Error:',err)
})
console.log('End')
