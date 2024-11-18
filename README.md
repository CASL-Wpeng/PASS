
# **Persistent Spiral Storage**

This repo is used for "Persistent Spiral Storage" published in "The 42nd IEEE International Conference on Computer Design (ICCD 2024)" 

## **Table of Contents**
- [Introduction](#introduction)
- [Paper Details](#paper-details)
- [Repository Structure](#repository-structure)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Acknowledgments](#acknowledgments)
- [License](#license)

---

## **Introduction**
This repository contains the code, data, and supplementary materials for our paper:

> [**Persistent Spiral Storage**](link-to-paper)  
> *Authors*: [Wenyu Peng](your-link), Tao Xie, Paul H. Siegel

Presented at **ICCD 2024 (The 42nd IEEE International Conference on Computer Design)**.

---

## **Paper Details**
**Citation:**  

Currently unavaliable

**Abstract:**  
Spiral storage is a dynamic hashing scheme proposed several decades ago. It has been largely overlooked in the recent adaptation of disk/DRAM-oriented dynamic hashing schemes to persistent memory (PM). The main reason is that its computational complexity is higher than that of its two well-known peers (extendible hashing and linear hashing). After an in-depth analysis, however, we discover that spiral storage actually has a good potential for PM as it expands a hash table through address remapping, which leads to fewer PM reads and writes. To tap the potential of spiral storage on PM, we develop a persistent spiral storage called PASS (Persistence-Aware Spiral Storage), which is facilitated by a group of new/existing techniques. Further, we conduct a comprehensive evaluation of PASS on a multi-core server equipped with Intel Optane DC Persistent Memory Modules (DCPMM). The experimental results demonstrate that compared with four state-of-the-art schemes it exhibits comparable or even better performance and scalability. In addition, it achieves a similar load factor and requires the same amount of recovery time.

---

## **Repository Structure**
```
├── HashEvaluation/                  # Source code for Spiral Storage using Pibench
├── Spiral_Storage/                  # Source code for Spiral Storage using YCSB
└── README.md               # This README file
```

The current version only contains the traditional spiral storage on PM. The code for PASS will be updated shortly.

---

## **Setup and Installation**
1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/repository-name.git
   cd PASS
   ```

2. **Install dependencies:**
   ```bash
   make
   ```

3. **Additional setup instructions:**  
   For the YCSB dataset, please refer to [YCSB](https://github.com/brianfrankcooper/YCSB).

---

## **Usage**
1. **Running the main script:**  
   To run the code with Pibench:
   ```bash
   cd Pibench
   make
   sudo ./PiBench hashFunctionName -keys
   ```
   To run the code with YCSB:
   ```bash
   cd Spiral_Storage/src/Spiral
   make
   sudo ./Spiral
   ```
 
   
---

## **Acknowledgments**
This work was partially supported by the US National Science Foundation under grant CNS-1813485. 

---

## **License**
```plaintext
MIT License
Copyright (c) 2024 Wenyu
```
