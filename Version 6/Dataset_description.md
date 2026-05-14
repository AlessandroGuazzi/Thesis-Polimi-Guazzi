# Rigorous Description: "Next Day Wildfire Spread" Dataset

## 1. Dataset Overview
The "Next Day Wildfire Spread" dataset is a curated, large-scale, multivariate dataset of historical wildfires that aggregates nearly a decade of remote-sensing data across the United States. Its primary purpose is to serve as a benchmark for developing machine learning models capable of predicting wildfire propagation with a lead time of one day.

* **Geographic Coverage:** Contiguous United States.
* **Timeframe:** 2012 to 2020.
* **Total Samples:** 18,545 fire events.
* **Event Dynamics:** In 58% of the samples (10,798), the fire increases in size from time $t$ to $t+1$ day. In 39% (7,191 samples), the fire decreases in size. The remainder stay the same size.
* **Spatial Layout:** Data is extracted as 64 km × 64 km regions to capture typical active fire sizes.
* **Spatial Resolution:** All variables are aligned to a strict 1 km resolution.

---

## 2. Input Variables (Features at Time $t$)
The dataset models fire spread by combining a 2-D historical fire mask with 11 observational variables overlaid over the 2-D regions. 

**Fire Tracking:**
* **Previous Fire Mask:** A 2-D mask showing the locations of active fires at time $t$. 

**Topography:**
* **Elevation:** Sourced from the Shuttle Radar Topography Mission (SRTM) at 30 m resolution, downsampled to 1 km.

**Weather & Climate (GRIDMET Data):**
* **Wind Direction & Wind Speed:** Daily surface fields.
* **Minimum & Maximum Temperature:** Daily surface fields.
* **Humidity & Precipitation:** Daily surface fields.
* **Drought Index:** Sourced from GRIDMET Drought, sampled every five days.
* *(Note: Weather variables are averaged over the day corresponding to time $t$ and interpolated using bicubic interpolation.)*

**Fuel & Vegetation:**
* **Vegetation (NDVI):** Sourced from the NASA VIIRS Vegetation Indices (VNP13A1), sampled every eight days, downsampled to 1 km.
* **Energy Release Component (ERC):** A calculated output from the National Fire Danger Rating System (NFDRS) acting as a composite fuel moisture index.

**Anthropogenic Proxies:**
* **Population Density:** Sourced from the Gridded Population of World Version 4 (GPWv4). Used as a proxy for human activity, as humans cause 84% of fires.

---

## 3. Target Variable (Output Label at Time $t+1$)
The machine learning task is framed as a precise image segmentation problem.

* **Fire Mask:** The target label is the "fire mask" at time $t+1$ day, providing a snapshot of the fire spreading pattern.
* **Classes:** Each 1 km × 1 km area within the region is classified as:
    * **Fire** 
    * **No Fire** 
    * **Uncertain:** For missing data, cloud coverage, or other unprocessed data. (These uncertain labels are ignored in loss and performance calculations ).

---

## 4. Sample Independence and Correlation Controls
To prevent temporal and spatial data leakage (i.e., the model memorizing a specific continuous fire event rather than learning general spread mechanics), strict aggregation rules were applied:

* **Temporal Splitting (The One-Day Buffer):** The dataset is split into training, evaluating, and testing sets according to an 8:1:1 ratio by randomly separating all weeks between 2012 and 2020. Crucially, a one-day buffer is kept between weeks from which no data is sampled to prevent correlation between sets.
* **Spatial Independence:** Fires separated by more than 10 km are treated as entirely different fire events.
* **Baseline Condition:** To guarantee the dataset characterizes *spreading* rather than just spontaneous ignition, a sample is only kept if the "previous fire mask" at time $t$ contains at least one area currently on fire.

---

## 5. Preprocessing & Normalization
Prior to ingestion by machine learning models, the data features undergo standardized preprocessing:

* **Clipping:** Extreme values are clipped to prevent vanishing or exploding gradients during training. The clipping thresholds are based on either physical knowledge (e.g., 0% to 100%) or set to the 0.1th and 99.9th percentiles for each specific feature.
* **Normalization:** Following clipping, each feature is normalized separately by subtracting its mean and dividing by its standard deviation. 
* **Data Augmentation:** Because fires naturally center within the 64 km × 64 km extracted regions, the input pipeline utilizes data augmentation by randomly cropping 32 km × 32 km regions from the original sample to simulate fires occurring at varying locations.
