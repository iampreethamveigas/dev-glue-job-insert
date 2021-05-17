var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var glue = new AWS.Glue();
const readline = require('readline');
const sourceBucket = process.env.sourceBucket;
const targetBucket = process.env.targetBucket;


exports.handler = async (event) => {
    listAllFiles(sourceBucket, callGlueJob);
    // TODO implement
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
};


/** Start the ETL glue job to insert data to vehicle_data table 
  * Source data catalog table name is sent as a parameter to the glue job */

function callGlueJob(filename) {
    var tablecsv = filename.substr(0, filename.length - 4).concat('_csv').toLowerCase();

    var params = {
        JobName: process.env.Jobname,
        Arguments: { '--sourceFile': tablecsv }

    };
    //Invoke job run
    glue.startJobRun(params, function (err, data) {

        if (err) console.log(err, err.stack); // an error occurred
        else
            console.log(data);           // successful response
        var jobId = data['JobRunId']
        //var dt = dateTime.create();
        //var formatted = dt.format('yyyyMMddHHmmSSsss');
        console.log(data['Status']);
        let ts = Date.now();
        moveTheFile(filename, 'archive/' + filename.concat('_').concat(ts), false); //moving the source file to archive folder  
        deleteInputFile(sourceBucket, 'processing/' + filename); //deleting the file from processing folder
        //deleteInputFile(targetBucket,filename.substr(0,filename.length-4).concat('.csv')); // deleting the csv file from target folder
    });

}



/** List all files from Source bucket
   *  Only text files will be processed **/
function listAllFiles(bucketName) {

    var count = 0;
    var filename;
    var params = {
        Bucket: bucketName /* required */
    };
    s3.listObjectsV2(params, function (err, data) {

        for (var i = 0; i < data.Contents.length; i++) {
            filename = data.Contents[i].Key;
            var key = data.Contents[i].Key.toUpperCase().endsWith('TXT');
            if (key) {
                console.log('while key ' + filename);
                readFile(bucketName, filename, readFileContent, onError);

                moveTheFile(filename, 'processing/' + filename, deleteInputFile, true); // Move the file to processing folder and then deletes it from input folder
                callGlueJob(filename);//Call the Glue job

            }

        }

        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(data);// successful response

        }
    });
    return count;
}

// Getting the filename from the source bucket 
function readFile(bucketName, filename, onFileContent, onError) {

    var params = { Bucket: bucketName, Key: filename };
    console.log("readFile " + filename);
    s3.getObject(params, function (err, data) {
        if (!err)
            onFileContent(filename, data.Body.toString());
        else
            console.log(err);
    });

}

/** Reading the fixed length file content 
 * Column lengths are hardcoded 
 * Converting the file to csv file format 
 * Append the filename to the csv file */

function readFileContent(filename, content) {
    //const IN_FILE = filename,
    //OUT_FILE = 'dataMartInput.csv',
    const BUFFER_LINES = 0;

    const RANGES = [[0, 7], [7, 4], [11, 2], [13, 14], [27, 1], [28, 1], [29, 1], [30, 1], [31, 1], [32, 1], [33, 1], [34, 1], [35, 1], [36, 1], [37, 1], [38, 1], [39, 1], [40, 1], [41, 17], [58, 3], [61, 4], [65, 4], [69, 4], [73, 4], [77, 6], [83, 2], [85, 3], [88, 2], [90, 3], [93, 6], [99, 5], [104, 3], [107, 3], [110, 10], [120, 6], [126, 8], [134, 4], [138, 4], [142, 4], [146, 4], [150, 3], [154, 200], [354, 1], [355, 12], [367, 2], [154, 1], [155, 1], [156, 1], [157, 1], [158, 1], [158, 1], [159, 1], [160, 1], [161, 1], [162, 1], [163, 1], [164, 1], [165, 1], [166, 1], [167, 1], [168, 1], [169, 1], [170, 1], [171, 1], [172, 1], [173, 1], [174, 1], [175, 1], [176, 1], [177, 1], [178, 1], [179, 1], [180, 1], [181, 1], [182, 1], [183, 1], [184, 1], [185, 1], [186, 1], [187, 1], [188, 1], [189, 1], [190, 1], [191, 1], [192, 1], [193, 1], [194, 1], [195, 1], [196, 1], [197, 1], [198, 1], [199, 1], [200, 1], [201, 1], [202, 1], [203, 1], [204, 1], [205, 1], [206, 1], [207, 1], [208, 1], [209, 1], [210, 1], [211, 1], [212, 1], [213, 1], [214, 1], [215, 1], [216, 1], [217, 1], [218, 1], [219, 1], [220, 1], [221, 1], [222, 1], [223, 1], [224, 1], [225, 1], [226, 1], [227, 1], [228, 1], [229, 1], [230, 1], [231, 1], [232, 1], [233, 1], [234, 1], [235, 1], [236, 1], [237, 1], [238, 1], [239, 1], [240, 1], [241, 1], [242, 1], [243, 1], [244, 1], [245, 1], [246, 1], [247, 1], [248, 1], [249, 1], [250, 1], [251, 1], [252, 1], [253, 1], [254, 1], [255, 1], [256, 1], [257, 1], [258, 1], [259, 1], [260, 1], [261, 1], [262, 1], [263, 1], [264, 1], [265, 1], [266, 1], [267, 1], [268, 1], [269, 1], [270, 1], [271, 1], [272, 1], [273, 1], [274, 1], [275, 1]
        , [276, 1], [277, 1], [278, 1], [279, 1], [280, 1], [281, 1], [282, 1], [283, 1], [284, 1], [285, 1], [286, 1], [287, 1], [288, 1], [289, 1], [290, 1], [291, 1], [292, 1], [293, 1], [294, 1], [295, 1], [296, 1], [297, 1], [298, 1], [299, 1], [300, 1], [301, 1], [302, 1], [303, 1], [304, 1], [305, 1], [306, 1], [307, 1], [308, 1], [309, 1], [310, 1], [311, 1], [312, 1], [313, 1], [314, 1], [315, 1], [316, 1], [317, 1], [318, 1], [319, 1], [320, 1], [321, 1], [322, 1], [323, 1], [324, 1], [325, 1], [326, 1], [327, 1], [328, 1], [329, 1], [330, 1], [331, 1], [332, 1], [333, 1], [334, 1], [335, 1], [336, 1], [337, 1], [338, 1], [339, 1], [340, 1], [341, 1], [342, 1], [343, 1], [344, 1], [345, 1], [346, 1], [347, 1], [348, 1], [349, 1], [350, 1], [351, 1], [352, 1], [353, 1], [354, 1]
        , [355, 12], [367, 2], [380, 30], [410, 10], [420, 4], [424, 4], [428, 4], [432, 4], [436, 4], [440, 5], [445, 5], [450, 2], [452, 5], [457, 2], [459, 12], [471, 12]];
    //for(let i=1; i<=245; i++){
    //  HEADERS.push("ref_no,status,ssn,katashiki,kat01,kat02,kat03,kat04,kat05,kat06,kat07,kat08,kat09,kat10,kat11,kat12,kat13,kat14,vin,ext,int,yymm_cal,yymm_pass,prod_week,lineoff_date,prod_path,region,dest,ctry_code,model_period,model,frame_type,machine_type,urn,eta_lineoff,int_oder_no,scr01,src02,src03,src04,src05,spec200_total,category,group_no,suffix,filename");
    //}


    var Readable = require('stream').Readable;
    var s = new Readable();

    s.push(content);    // the string you want
    s.push(null);      // indicates end-of-file basically - the end of the stream
    const instream = s,
        //outstream = fs.createWriteStream(OUT_FILE),
        rl = readline.createInterface({ input: instream, crlfDelay: Infinity });
    let buffer = []; //HEADERS.join(",")+"\n";  
    //var bufferedLines = 2000;
    instream.on('error', (e) => { console.error(e.message); });
    //outstream.on('error', (e)=>{console.error(e.message);});
    //rl.on('line', function(line) {
    rl.on('line', (line) => {
        // console.log("line-----" + line);
        var parts = [];
        for (let range of RANGES)
            parts.push(line.substr(range[0], range[1]).trim());
        buffer += parts.join(",").concat(",").concat(filename.substr(0, filename.length - 4)) + "\n";
        //buffer += parts.join(",")+"\n";

    });
    rl.on('close', () => {
        var outputFileName = filename.substr(0, filename.length - 4).concat('.csv');
        console.log("outputFilename " + outputFileName);
        createWriteStream(targetBucket, outputFileName, buffer);


    });
}

function onError(err) {
    console.log('error: ' + err);

}

// Uploading the csv file to the target bucket

function createWriteStream(Bucket, Key, outstream) {
    console.log("outstream " + Bucket);
    s3
        .upload({
            Bucket,
            Key,
            Body: outstream

        })
        .promise();
    return { outstream };
}

function deleteInputFile(bucketName, fileName) {
    console.log('inside delete');

    var params = {
        Bucket: bucketName,
        Key: fileName
    };

    s3.deleteObject(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
    }).promise();
}

function moveTheFile(fileName, targetLoc, deleteFlag) {
    console.log('inside move** ' + targetLoc);

    var params = {
        Bucket: targetBucket, /* Another bucket working fine */
        CopySource: sourceBucket + '/' + fileName,
        Key: targetLoc,
    };

    s3.copyObject(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(data);// successful response
            if (deleteFlag)
                deleteInputFile(sourceBucket, fileName);
        }
    });
}
