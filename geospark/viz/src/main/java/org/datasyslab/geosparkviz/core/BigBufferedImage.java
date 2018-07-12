/*
 * FILE: BigBufferedImage
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparkviz.core;

/*
 * This class is part of MCFS (Mission Control - Flight Software) a development
 * of Team Puli Space, official Google Lunar XPRIZE contestant.
 * This class is released under Creative Commons CC0.
 * @author Zsolt Pocze, Dimitry Polivaev
 * Please like us on facebook, and/or join our Small Step Club.
 * http://www.pulispace.com
 * https://www.facebook.com/pulispace
 * http://nyomdmegteis.hu/en/
 */

import sun.nio.ch.DirectBuffer;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.color.ColorSpace;
import java.awt.image.BandedSampleModel;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO: Auto-generated Javadoc

/**
 * The Class BigBufferedImage.
 */
public class BigBufferedImage
        extends BufferedImage
{

    /**
     * The Constant TMP_DIR.
     */
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * The Constant MAX_PIXELS_IN_MEMORY.
     */
    public static final int MAX_PIXELS_IN_MEMORY = 1024 * 1024;

    /**
     * Creates the.
     *
     * @param width the width
     * @param height the height
     * @param imageType the image type
     * @return the buffered image
     */
    public static BufferedImage create(int width, int height, int imageType)
    {
        if (width * height > MAX_PIXELS_IN_MEMORY) {
            try {
                final File tempDir = new File(TMP_DIR);
                return createBigBufferedImage(tempDir, width, height, imageType);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            return new BufferedImage(width, height, imageType);
        }
    }

    /**
     * Creates the.
     *
     * @param inputFile the input file
     * @param imageType the image type
     * @return the buffered image
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static BufferedImage create(File inputFile, int imageType)
            throws IOException
    {
        try (ImageInputStream stream = ImageIO.createImageInputStream(inputFile);) {
            Iterator<ImageReader> readers = ImageIO.getImageReaders(stream);
            if (readers.hasNext()) {
                try {
                    ImageReader reader = readers.next();
                    reader.setInput(stream, true, true);
                    int width = reader.getWidth(reader.getMinIndex());
                    int height = reader.getHeight(reader.getMinIndex());
                    BufferedImage image = create(width, height, imageType);
                    int cores = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
                    int block = Math.min(MAX_PIXELS_IN_MEMORY / cores / width, (int) (Math.ceil(height / (double) cores)));
                    ExecutorService generalExecutor = Executors.newFixedThreadPool(cores);
                    List<Callable<ImagePartLoader>> partLoaders = new ArrayList<>();
                    for (int y = 0; y < height; y += block) {
                        partLoaders.add(new ImagePartLoader(
                                y, width, Math.min(block, height - y), inputFile, image));
                    }
                    generalExecutor.invokeAll(partLoaders);
                    generalExecutor.shutdown();
                    return image;
                }
                catch (InterruptedException ex) {
                    Logger.getLogger(BigBufferedImage.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return null;
    }

    /**
     * Creates the big buffered image.
     *
     * @param tempDir the temp dir
     * @param width the width
     * @param height the height
     * @param imageType the image type
     * @return the buffered image
     * @throws FileNotFoundException the file not found exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static BufferedImage createBigBufferedImage(File tempDir, int width, int height, int imageType)
            throws FileNotFoundException, IOException
    {
        FileDataBuffer buffer = new FileDataBuffer(tempDir, width * height, 4);
        ColorModel colorModel = null;
        BandedSampleModel sampleModel = null;
        switch (imageType) {
            case TYPE_INT_RGB:
                colorModel = new ComponentColorModel(ColorSpace.getInstance(ColorSpace.CS_sRGB),
                        new int[] {8, 8, 8, 0},
                        false,
                        false,
                        ComponentColorModel.TRANSLUCENT,
                        DataBuffer.TYPE_BYTE);
                sampleModel = new BandedSampleModel(DataBuffer.TYPE_BYTE, width, height, 3);
                break;
            case TYPE_INT_ARGB:
                colorModel = new ComponentColorModel(ColorSpace.getInstance(ColorSpace.CS_sRGB),
                        new int[] {8, 8, 8, 8},
                        true,
                        false,
                        ComponentColorModel.TRANSLUCENT,
                        DataBuffer.TYPE_BYTE);
                sampleModel = new BandedSampleModel(DataBuffer.TYPE_BYTE, width, height, 4);
                break;
            default:
                throw new IllegalArgumentException("Unsupported image type: " + imageType);
        }
        SimpleRaster raster = new SimpleRaster(sampleModel, buffer, new Point(0, 0));
        BigBufferedImage image = new BigBufferedImage(colorModel, raster, colorModel.isAlphaPremultiplied(), null);
        return image;
    }

    /**
     * The Class ImagePartLoader.
     */
    private static class ImagePartLoader
            implements Callable<ImagePartLoader>
    {

        /**
         * The y.
         */
        private final int y;

        /**
         * The image.
         */
        private final BufferedImage image;

        /**
         * The region.
         */
        private final Rectangle region;

        /**
         * The file.
         */
        private final File file;

        /**
         * Instantiates a new image part loader.
         *
         * @param y the y
         * @param width the width
         * @param height the height
         * @param file the file
         * @param image the image
         */
        public ImagePartLoader(int y, int width, int height, File file, BufferedImage image)
        {
            this.y = y;
            this.image = image;
            this.file = file;
            region = new Rectangle(0, y, width, height);
        }

        /* (non-Javadoc)
         * @see java.util.concurrent.Callable#call()
         */
        @Override
        public ImagePartLoader call()
                throws Exception
        {
            Thread.currentThread().setPriority((Thread.MIN_PRIORITY + Thread.NORM_PRIORITY) / 2);
            try (ImageInputStream stream = ImageIO.createImageInputStream(file);) {
                Iterator<ImageReader> readers = ImageIO.getImageReaders(stream);
                if (readers.hasNext()) {
                    ImageReader reader = readers.next();
                    reader.setInput(stream, true, true);
                    ImageReadParam param = reader.getDefaultReadParam();
                    param.setSourceRegion(region);
                    BufferedImage part = reader.read(0, param);
                    Raster source = part.getRaster();
                    WritableRaster target = image.getRaster();
                    target.setRect(0, y, source);
                }
            }
            return ImagePartLoader.this;
        }
    }

    /**
     * Instantiates a new big buffered image.
     *
     * @param cm the cm
     * @param raster the raster
     * @param isRasterPremultiplied the is raster premultiplied
     * @param properties the properties
     */
    private BigBufferedImage(ColorModel cm, SimpleRaster raster, boolean isRasterPremultiplied, Hashtable<?, ?> properties)
    {
        super(cm, raster, isRasterPremultiplied, properties);
    }

    /**
     * Dispose.
     */
    public void dispose()
    {
        ((SimpleRaster) getRaster()).dispose();
    }

    /**
     * Dispose.
     *
     * @param image the image
     */
    public static void dispose(RenderedImage image)
    {
        if (image instanceof BigBufferedImage) {
            ((BigBufferedImage) image).dispose();
        }
    }

    /**
     * The Class SimpleRaster.
     */
    private static class SimpleRaster
            extends WritableRaster
    {

        /**
         * Instantiates a new simple raster.
         *
         * @param sampleModel the sample model
         * @param dataBuffer the data buffer
         * @param origin the origin
         */
        public SimpleRaster(SampleModel sampleModel, FileDataBuffer dataBuffer, Point origin)
        {
            super(sampleModel, dataBuffer, origin);
        }

        /**
         * Dispose.
         */
        public void dispose()
        {
            ((FileDataBuffer) getDataBuffer()).dispose();
        }
    }

    /**
     * The Class FileDataBufferDeleterHook.
     */
    private static final class FileDataBufferDeleterHook
            extends Thread
    {

        static {
            Runtime.getRuntime().addShutdownHook(new FileDataBufferDeleterHook());
        }

        /**
         * The Constant undisposedBuffers.
         */
        private static final HashSet<FileDataBuffer> undisposedBuffers = new HashSet<>();

        /* (non-Javadoc)
         * @see java.lang.Thread#run()
         */
        @Override
        public void run()
        {
            final FileDataBuffer[] buffers = undisposedBuffers.toArray(new FileDataBuffer[0]);
            for (FileDataBuffer b : buffers) {
                b.disposeNow();
            }
        }
    }

    /**
     * The Class FileDataBuffer.
     */
    private static class FileDataBuffer
            extends DataBuffer
    {

        /**
         * The id.
         */
        private final String id = "buffer-" + System.currentTimeMillis() + "-" + ((int) (Math.random() * 1000));

        /**
         * The dir.
         */
        private File dir;

        /**
         * The path.
         */
        private String path;

        /**
         * The files.
         */
        private File[] files;

        /**
         * The access files.
         */
        private RandomAccessFile[] accessFiles;

        /**
         * The buffer.
         */
        private MappedByteBuffer[] buffer;

        /**
         * Instantiates a new file data buffer.
         *
         * @param dir the dir
         * @param size the size
         * @throws FileNotFoundException the file not found exception
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public FileDataBuffer(File dir, int size)
                throws FileNotFoundException, IOException
        {
            super(TYPE_BYTE, size);
            this.dir = dir;
            init();
        }

        /**
         * Instantiates a new file data buffer.
         *
         * @param dir the dir
         * @param size the size
         * @param numBanks the num banks
         * @throws FileNotFoundException the file not found exception
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public FileDataBuffer(File dir, int size, int numBanks)
                throws FileNotFoundException, IOException
        {
            super(TYPE_BYTE, size, numBanks);
            this.dir = dir;
            init();
        }

        /**
         * Inits the.
         *
         * @throws FileNotFoundException the file not found exception
         * @throws IOException Signals that an I/O exception has occurred.
         */
        private void init()
                throws FileNotFoundException, IOException
        {
            FileDataBufferDeleterHook.undisposedBuffers.add(this);
            if (dir == null) {
                dir = new File(".");
            }
            if (!dir.exists()) {
                throw new RuntimeException("FileDataBuffer constructor parameter dir does not exist: " + dir);
            }
            if (!dir.isDirectory()) {
                throw new RuntimeException("FileDataBuffer constructor parameter dir is not a directory: " + dir);
            }
            path = dir.getPath() + "/" + id;
            File subDir = new File(path);
            subDir.mkdir();
            buffer = new MappedByteBuffer[banks];
            accessFiles = new RandomAccessFile[banks];
            files = new File[banks];
            for (int i = 0; i < banks; i++) {
                File file = files[i] = new File(path + "/bank" + i + ".dat");
                final RandomAccessFile randomAccessFile = accessFiles[i] = new RandomAccessFile(file, "rw");
                buffer[i] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, getSize());
            }
        }

        /* (non-Javadoc)
         * @see java.awt.image.DataBuffer#getElem(int, int)
         */
        @Override
        public int getElem(int bank, int i)
        {
            return buffer[bank].get(i) & 0xff;
        }

        /* (non-Javadoc)
         * @see java.awt.image.DataBuffer#setElem(int, int, int)
         */
        @Override
        public void setElem(int bank, int i, int val)
        {
            buffer[bank].put(i, (byte) val);
        }

        /* (non-Javadoc)
         * @see java.lang.Object#finalize()
         */
        @Override
        protected void finalize()
                throws Throwable
        {
            dispose();
        }

        /**
         * Dispose now.
         */
        private void disposeNow()
        {
            final MappedByteBuffer[] disposedBuffer = this.buffer;
            this.buffer = null;
            disposeNow(disposedBuffer);
        }

        /**
         * Dispose.
         */
        public void dispose()
        {
            final MappedByteBuffer[] disposedBuffer = this.buffer;
            this.buffer = null;
            new Thread()
            {
                @Override
                public void run()
                {
                    disposeNow(disposedBuffer);
                }
            }.start();
        }

        /**
         * Dispose now.
         *
         * @param disposedBuffer the disposed buffer
         */
        private void disposeNow(final MappedByteBuffer[] disposedBuffer)
        {
            FileDataBufferDeleterHook.undisposedBuffers.remove(this);
            if (disposedBuffer != null) {
                for (MappedByteBuffer b : disposedBuffer) {
                    ((DirectBuffer) b).cleaner().clean();
                }
            }
            if (accessFiles != null) {
                for (RandomAccessFile file : accessFiles) {
                    try {
                        file.close();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                accessFiles = null;
            }
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
                files = null;
            }
            if (path != null) {
                new File(path).delete();
                path = null;
            }
        }
    }
}