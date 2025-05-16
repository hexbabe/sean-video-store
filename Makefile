SOURCE_OS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
SOURCE_ARCH ?= $(shell uname -m)
TARGET_OS ?= $(SOURCE_OS)
TARGET_ARCH ?= $(SOURCE_ARCH)
normalize_arch = $(if $(filter aarch64,$(1)),arm64,$(if $(filter x86_64,$(1)),amd64,$(1)))
SOURCE_ARCH := $(call normalize_arch,$(SOURCE_ARCH))
TARGET_ARCH := $(call normalize_arch,$(TARGET_ARCH))

BIN_OUTPUT_PATH = bin/$(TARGET_OS)-$(TARGET_ARCH)
TOOL_BIN = bin/gotools/$(shell uname -s)-$(shell uname -m)

FFMPEG_TAG ?= n6.1
FFMPEG_VERSION ?= $(shell pwd)/FFmpeg/$(FFMPEG_TAG)
FFMPEG_VERSION_PLATFORM ?= $(FFMPEG_VERSION)/$(TARGET_OS)-$(TARGET_ARCH)
FFMPEG_BUILD ?= $(FFMPEG_VERSION_PLATFORM)/build
FFMPEG_LIBS=    libavformat                        \
                libavcodec                         \
                libavutil                          \
                libswscale                          \

FFMPEG_OPTS ?= --prefix=$(FFMPEG_BUILD) \
               --disable-shared \
               --disable-programs \
               --disable-doc \
               --disable-everything \
               --enable-static \
               --enable-libx264 \
               --enable-decoder=hevc \
               --enable-decoder=h264 \
               --enable-gpl \
               --enable-encoder=libx264 \
               --enable-muxer=segment \
               --enable-muxer=mp4 \
               --enable-demuxer=segment \
               --enable-demuxer=concat \
               --enable-demuxer=mov \
               --enable-demuxer=mp4 \
               --enable-parser=h264 \
               --enable-parser=hevc \
               --enable-protocol=file \
               --enable-protocol=concat \
               --enable-protocol=crypto \
               --enable-bsf=h264_mp4toannexb \
               --enable-bsf=hevc_mp4toannexb \
               --enable-decoder=mjpeg

GOFLAGS := -buildvcs=false
SRC_DIR := videostore
BUILD_DIR := build/$(TARGET_OS)-$(TARGET_ARCH)
SRCS := $(shell find $(SRC_DIR) -name '*.c')
OBJS := $(subst $(SRC_DIR), $(BUILD_DIR), $(SRCS:.c=.o))
PKG_CONFIG_PATH = $(FFMPEG_BUILD)/lib/pkgconfig
CGO_CFLAGS = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --cflags $(FFMPEG_LIBS))
ifeq ($(SOURCE_OS),linux)
	SUBST = -l:libx264.a
endif
ifeq ($(SOURCE_OS),darwin)
	SUBST = $(HOMEBREW_PREFIX)/Cellar/x264/r3108/lib/libx264.a
endif
CGO_LDFLAGS = $(subst -lx264, $(SUBST),$(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs $(FFMPEG_LIBS))) -ldl
export PATH := $(PATH):$(shell go env GOPATH)/bin

.PHONY: lint tool-install test clean clean-all clean-ffmpeg module build valgrind valgrind-run

all: $(FFMPEG_BUILD) $(BIN_OUTPUT_PATH)/video-store $(BIN_OUTPUT_PATH)/concat

$(BIN_OUTPUT_PATH)/video-store: videostore/*.go cmd/module/*.go videostore/*.c videostore/utils/*.c videostore/*.h videostore/utils/*.h $(FFMPEG_BUILD)
	go mod tidy
	CGO_LDFLAGS="$(CGO_LDFLAGS)" CGO_CFLAGS="$(CGO_CFLAGS)" go build -o $(BIN_OUTPUT_PATH)/video-store cmd/module/cmd.go

$(BIN_OUTPUT_PATH)/concat: videostore/*.go cmd/concat/*.go videostore/*.c videostore/utils/*.c videostore/*.h videostore/utils/*.h $(FFMPEG_BUILD)
	go mod tidy
	CGO_LDFLAGS="$(CGO_LDFLAGS)" CGO_CFLAGS="$(CGO_CFLAGS)" go build -o $(BIN_OUTPUT_PATH)/concat cmd/concat/cmd.go

AR = ar
$(BUILD_DIR)/libviamav.a:
	$(AR) crs $@ $(BUILD_DIR)/concat.o $(BUILD_DIR)/rawsegmenter.o

$(BIN_OUTPUT_PATH)/concat-c: $(FFMPEG_BUILD) $(OBJS) $(BUILD_DIR)/libviamav.a | $(BUILD_DIR) $(BIN_OUTPUT_PATH)
	@echo "-------- Make $(BIN_OUTPUT_PATH)/concat-c --------"
	rm -f $(BIN_OUTPUT_PATH)/concat-c
	mkdir -p $(BUILD_DIR)
	$(CC) $(OBJS) ./cmd/concat-c/main.c $(CGO_LDFLAGS) -ldl -L$(BUILD_DIR) -lviamav $(CGO_CFLAGS)  -g -o $(BIN_OUTPUT_PATH)/concat-c

$(BIN_OUTPUT_PATH)/encoder-c: $(FFMPEG_BUILD) $(OBJS) $(BUILD_DIR)/libviamav.a | $(BUILD_DIR) $(BIN_OUTPUT_PATH)
	@echo "-------- Make $(BIN_OUTPUT_PATH)/concat-c --------"
	rm -f $(BIN_OUTPUT_PATH)/encoder-c
	mkdir -p $(BUILD_DIR)
	$(CC) $(OBJS) ./cmd/encode-c/main.c $(CGO_LDFLAGS) $(shell pkg-config --cflags sqlite3) -ldl -L$(BUILD_DIR) -lviamav $(CGO_CFLAGS)  $(shell pkg-config --libs sqlite3) -g -o $(BIN_OUTPUT_PATH)/encoder-c


$(BIN_OUTPUT_PATH)/raw-segmenter-c: $(FFMPEG_BUILD) $(OBJS) $(BUILD_DIR)/libviamav.a | $(BUILD_DIR) $(BIN_OUTPUT_PATH)
	@echo "-------- Make $(BIN_OUTPUT_PATH)/concat-c --------"
	rm -f $(BIN_OUTPUT_PATH)/raw-segmenter-c
	mkdir -p $(BUILD_DIR)
	$(CC) $(OBJS) ./cmd/raw-segmenter-c/main.c $(CGO_LDFLAGS) $(shell pkg-config --cflags sqlite3) -ldl -L$(BUILD_DIR) -lviamav $(CGO_CFLAGS)  $(shell pkg-config --libs sqlite3) -g -o $(BIN_OUTPUT_PATH)/raw-segmenter-c

$(BIN_OUTPUT_PATH)/video-info-c: $(FFMPEG_BUILD) $(OBJS) | $(BUILD_DIR) $(BIN_OUTPUT_PATH)
	@echo "-------- Make $(BIN_OUTPUT_PATH)/video-info-c --------"
	rm -f $(BIN_OUTPUT_PATH)/video-info-c
	mkdir -p $(BUILD_DIR)
	$(CC) $(OBJS) ./cmd/video-info-c/main.c $(CGO_LDFLAGS) -ldl $(CGO_CFLAGS) -g -o $(BIN_OUTPUT_PATH)/video-info-c

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	@echo "-------- Make $(@) --------"
	rm -f $@
	$(CC) $(CGO_LDFLAGS) $(CGO_CFLAGS) -g -c -o $@ $<

$(BUILD_DIR):
	@echo "-------- mkdir $(@) --------"
	mkdir -p $(BUILD_DIR)

$(BIN_OUTPUT_PATH):
	@echo "-------- mkdir $(@) --------"
	mkdir -p $(BIN_OUTPUT_PATH)

$(FFMPEG_VERSION_PLATFORM):
	git clone https://github.com/FFmpeg/FFmpeg.git --depth 1 --branch $(FFMPEG_TAG) $(FFMPEG_VERSION_PLATFORM)

$(FFMPEG_BUILD): $(FFMPEG_VERSION_PLATFORM)
# Only need nasm to build assembly kernels for amd64 targets.
ifeq ($(SOURCE_OS),linux)
ifeq ($(shell dpkg -l | grep -w x264 > /dev/null; echo $$?), 1)
	sudo apt update && sudo apt install -y libx264-dev
endif
ifeq ($(SOURCE_ARCH),amd64)
	which nasm || (sudo apt update && sudo apt install -y nasm)
endif
endif
ifeq ($(SOURCE_OS),darwin)
ifeq ($(shell brew list | grep -w x264 > /dev/null; echo $$?), 1)
	brew update && brew install x264
endif
endif
	cd $(FFMPEG_VERSION_PLATFORM) && ./configure $(FFMPEG_OPTS) && $(MAKE) -j$(NPROC) && $(MAKE) install

tool-install:
	GOBIN=`pwd`/$(TOOL_BIN) go install \
		github.com/edaniels/golinters/cmd/combined \
		github.com/golangci/golangci-lint/cmd/golangci-lint \
		github.com/rhysd/actionlint/cmd/actionlint

lint: tool-install $(FFMPEG_BUILD)
	go mod tidy
	CGO_CFLAGS=$(CGO_CFLAGS) GOFLAGS=$(GOFLAGS) $(TOOL_BIN)/golangci-lint run -v --fix --config=./etc/.golangci.yaml --timeout=2m

test: $(BIN_OUTPUT_PATH)/video-store
ifeq ($(shell which ffmpeg > /dev/null 2>&1; echo $$?), 1)
ifeq ($(SOURCE_OS),linux)
	sudo apt update && sudo apt install -y ffmpeg
endif
ifeq ($(SOURCE_OS),darwin)
	brew update && brew install ffmpeg
endif
endif
ifeq ($(shell which artifact > /dev/null 2>&1; echo $$?), 1)
	go install go.viam.com/utils/artifact/cmd/artifact@latest
endif
	artifact pull
	cp $(BIN_OUTPUT_PATH)/video-store bin/video-store
	CGO_LDFLAGS="$(CGO_LDFLAGS)" CGO_CFLAGS=$(CGO_CFLAGS) go test -v ./...
	rm bin/video-store

module: $(BIN_OUTPUT_PATH)/video-store
	cp $(BIN_OUTPUT_PATH)/video-store bin/video-store
	tar czf module.tar.gz bin/video-store
	rm bin/video-store

clean:
	rm -rf bin
	rm -rf build

clean-ffmpeg: clean
	rm -rf FFmpeg

clean-all: clean clean-ffmpeg
	git clean -fxd

valgrind-setup: 
ifeq ($(shell which valgrind > /dev/null 2>&1; echo $$?), 1)
ifeq ($(SOURCE_OS),linux)
	wget https://sourceware.org/pub/valgrind/valgrind-3.24.0.tar.bz2
	tar xjf valgrind-3.24.0.tar.bz2
	rm -rf valgrind-3.24.0.tar.bz2
	cd valgrind-3.24.0
	cd valgrind-3.24.0 && ./configure && make && sudo make install
	rm -rf valgrind-3.24.0.tar.bz2
	rm -rf valgrind-3.24.0
endif
ifeq ($(SOURCE_OS),darwin)
	echo "valgrind not supported on macos building in canon"
	canon make valgrind-setup
endif
endif

# you need latest valgrind, otherwise you might not get line numbers in your valgrind output
valgrind-run: TARGET=$(BIN_OUTPUT_PATH)/$(PROGRAM)
valgrind-run: 
ifeq ($(SOURCE_OS),linux)
	sudo apt-get install libc6-dbg
	valgrind --error-exitcode=1 --leak-check=full --track-origins=yes --dsymutil=yes -v $(TARGET) $(ARGS)
endif
ifeq ($(SOURCE_OS),darwin)
	echo "valgrind not supported on macos running in canon"
	canon make clean ./bin/linux-arm64/raw-segmenter-c valgrind-run
endif

run:
ifeq ($(SOURCE_OS),linux)
	./bin/linux-arm64/raw-segmenter-c my.db
endif
ifeq ($(SOURCE_OS),darwin)
	echo "valgrind not supported on macos running in canon"
	canon make clean ./bin/linux-arm64/raw-segmenter-c run
endif

gdb-run: 
ifeq ($(SOURCE_OS),linux)
	gdb ./bin/linux-arm64/raw-segmenter-c 
endif
ifeq ($(SOURCE_OS),darwin)
	echo "valgrind not supported on macos running in canon"
	canon make clean ./bin/linux-arm64/raw-segmenter-c gdb-run
endif
