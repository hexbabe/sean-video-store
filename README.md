# [`video-store` camera](https://app.viam.com/module/viam/video-store)

The `video-store` module brings security camera functionality to your smart machine! The module consumes a source [Camera](https://docs.viam.com/components/camera/) and saves the output as video files on disk. You can then upload video slices to the cloud using the [save](#save) command, or request the video bytes directly using the [fetch](#fetch) command.

Navigate to the [**CONFIGURE** tab](https://docs.viam.com/configure/) of your [machine](https://docs.viam.com/fleet/machines/) in [the Viam app](https://app.viam.com/).
[Add camera / video-store to your machine](https://docs.viam.com/configure/#components).

## Requirements

### Configure a Data Manager Service

Make sure to configure a [Data Manager Service](https://docs.viam.com/services/data/cloud-sync/) to upload video files to the cloud when saving video slices.

Navigate to the [**CONFIGURE** tab](https://docs.viam.com/configure/) of your [machine](https://docs.viam.com/fleet/machines/) in [the Viam app](https://app.viam.com/). [Add data-management to your machine](https://docs.viam.com/configure/#services).

> [!NOTE]
> The `additional_sync_paths` attribute must include the custom path specified in the `upload_path` attribute of the `video-store` component if it is not under `~/.viam/capture`.

```json
    {
      "name": <data_manager_service_name>,
      "namespace": "rdk",
      "type": "data_manager",
      "attributes": {
        "tags": [],
        "additional_sync_paths": [
          <custom_path_to_upload_video_files>
        ],
        "capture_disabled": true,
        "sync_interval_mins": <sync_interval_minutes>,
        "capture_dir": ""
      }
    }
```

## Configure your `video-store` component

Video store requires a [Data Manager](https://docs.viam.com/data-ai/capture-data/capture-sync/#configure-the-data-management-service-for-your-machine) to be configured on you machine before it can be used.
Use the "depends_on" dropdown to add a dependency on the datamanger configured on your machine. 


Copy and paste the following attributes into your JSON configuration:
```json
{
  "camera": "<source-camera-name>",
  "sync": "<data-manager-service-name>",
  "storage": {
    "size_gb": <int>
  }
}
```



Additionally, make sure to add your configured data manager service to the `depends_on` array of your `video-store` component.

### Attributes

| Attribute       | Sub-Attribute     | Type    | Required | Description                                                                                       |
|-----------------|-------------------|---------|-----------|---------------------------------------------------------------------------------------------------|
| `camera`        |                   | string  | no  | Name of the source camera to read images from. If not provided, video-store will not save video.  |
| `sync`          |                   | string  | yes  | Name of the dependency datamanager service.                                                       |
| `storage`       |                   | object  | yes  |                                                                                                   |
|                 | `size_gb`         | integer | yes  | Total amount of allocated storage in gigabytes. If you reduce the amound of allocated storage while the storage exceeds the allocated amount, the oldest clips get deleted until the storage size is below the configured max. |
|                 | `storage_path`    | string  | no  | Custom path to use for video storage.                                                             |
|                 | `upload_path`     | string  | no  | Custom path to use for uploading files. If not under `~/.viam/capture`, you will need to add to `additional_sync_paths` in datamanager service configuration. |
| `video`         |                   | object  | no  |                                                                                                   |
|                 | `format`          | string  | no  | Name of video format to use (e.g., mp4).                                                          |
|                 | `codec`           | string  | no  | Name of video codec to use (e.g., h264).                                                          |
|                 | `bitrate`         | integer | no  | Throughput of encoder in bits per second. Higher for better quality video, and lower for better storage efficiency. |
|                 | `preset`          | string  | no  | Name of codec video preset to use. See [here](https://trac.ffmpeg.org/wiki/Encode/H.264#a2.Chooseapresetandtune) for preset options.                                                                |
| `framerate`     |                   | integer | no  | Frame rate of the video in frames per second. Default value is 20 if not set.                      |

### Example Configuration

```json
{
  "name": "video-store",
  "namespace": "rdk",
  "type": "camera",
  "model": "viam:video:storage",
  "attributes": {
    "camera": "wc-cam",
    "sync": "data-manager",
    "storage": {
      "size_gb": 50
    }
  },
  "depends_on": [
    "wc-cam",
    "data-manager"
  ]
}
```

## DoCommand API

### From/To

The `From` and `To` timestamps are used to specify the start and end times for video clips. These timestamps must be provided in a specific datetime format to ensure proper parsing and formatting.

#### Datetime Format

The datetime format used is:

- Local Time: `YYYY-MM-DD_HH-MM-SS`
- UTC Time: `YYYY-MM-DD_HH-MM-SSZ`

Where:
- `YYYY`: Year (e.g., 2023)
- `MM`: Month (e.g., 01 for January)
- `DD`: Day (e.g., 15)
- `HH`: Hour in 24-hour format (e.g., 14 for 2 PM)
- `MM`: Minutes (e.g., 30)
- `SS`: Seconds (e.g., 45)
- `Z`: Optional suffix indicating the time is in UTC.

#### Datetime Example

- `2024-01-15_14-30-45` represents January 15, 2024, at 2:30:45 PM **local time**.
- `2024-01-15_14-30-45Z` represents January 15, 2024, at 2:30:45 PM **UTC**.

### `Save`

The save command retreives video from local storage, concatenates and trims underlying storage segments based on time range, and uploads the clip to the cloud.

| Attribute   | Type                | Required/Optional | Description                      |
|-------------|---------------------|-------------------|----------------------------------|
| `command`   | string              | required          | Command to be executed.          |
| `from`      | timestamp           | required          | Start timestamp.                 |
| `to`        | timestamp           | required          | End timestamp.                   |
| `metadata`  | string              | optional          | Arbitrary metadata string.       |
| `async`     | boolean             | optional          | Whether the operation is async.  |

#### Save Request
```json
{
  "command": "save",
  "from": <start_timestamp>,
  "to": <end_timestamp>,
  "metadata": <arbitrary_metadata_string>
}
```

#### Save Response
```json
{
  "command": "save",
  "filename": <filename_to_be_uploaded>
}
```

#### Async Save Request

The async save command performs the same operation as the save command, but does not wait for the operation to complete. Use this command when you want to save video slices that include the current in-progress video storage segment. It will wait for the current segment to finish recording before saving the video slice.

> [!NOTE]
> The async save command does not support future timestamps. The `from` timestamp must be in the past.
> The `to` timestamp must be the current time or in the past.

```json
{
  "command": "save",
  "from": <start_timestamp>,
  "to": <end_timestamp>,
  "metadata": <arbitrary_metadata_string>,
  "async": true
}
```

#### Async Save Response
```json
{
  "command": "save",
  "filename": <filename_to_be_uploaded>,
  "status": "async"
}
```

### `Fetch`

The fetch command retrieves video from local storage, and sends the bytes directly back to the client.

| Attribute | Type       | Required/Optional | Description          |
|-----------|------------|-------------------|----------------------|
| `command` | string     | required          | Command to be executed. |
| `from`    | timestamp  | required          | Start timestamp.     |
| `to`      | timestamp  | required          | End timestamp.       |

#### Fetch Request
```json
{
  "command": "fetch",
  "from": <start_timestamp>,
  "to": <end_timestamp>
}
```

#### Fetch Response
```json
{
  "command": "fetch",
  "video": <video_bytes>
}
```

## Local Development

### Building

The [Makefile](./Makefile) supports building for the following platforms:

| Platform       | Architecture |
|----------------|--------------|
| `linux`        | `arm64`      |
| `linux`        | `amd64`      |
| `darwin`       | `arm64`      |

To build for linux/arm64, run the following commands:
```
canon -arch arm64
make
```

To build for linux/amd64, run the following commands:
```
canon -arch amd64
make
```

To build for darwin/arm64, run the following command on a Mac with an M series chip:
```
make
```

### Testing

Run [tests](./tests) to ensure the module is functioning as expected with the following command:

```
make test
```

### Linting

```
make lint
```

### Package module
  
```
make module
```
