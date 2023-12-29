use std::fmt;
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyTypeError, PyException};
use pyo3::types::PyBytes;
use prism_client::{Client as PrismClient,
                   Wavelet as PrismWavelet,
                   Photon as PrismPhoton};
use http::Uri;

#[pyclass]
#[derive(Clone, Debug)]
struct Photon {
    #[pyo3(get)]
    index: u64,
    #[pyo3(get)]
    time: i64,
    #[pyo3(get)]
    payload: PyObject
}

impl Photon {
    fn from_rust(py: Python, photon: &PrismPhoton) -> Self {
        Self {
          index: photon.index,
          time: photon.time,
          payload: PyBytes::new(py, photon.payload.as_slice()).to_object(py)
        }
    }
}

impl fmt::Display for Photon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Photon({}, {}, <payload>)",
               self.index,
               self.time)
    }
}

#[pymethods]
impl Photon {
    fn __repr__(&self) -> String {
        self.to_string()
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct Wavelet {
    #[pyo3(get)]
    beam: String,
    #[pyo3(get)]
    photons: Vec<Photon>
}

impl Wavelet {
    fn from_rust(py: Python, wavelet: &PrismWavelet) -> Self {
        let PrismWavelet{beam, photons} = wavelet;
        let photons = photons
            .iter()
            .map(|ph| Photon::from_rust(py, ph))
            .collect();
        Self { beam: beam.to_string(), photons }
    }
}

impl fmt::Display for Wavelet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Wavelet({}, {:?})", self.beam, self.photons)
    }
}

#[pymethods]
impl Wavelet {
    fn __repr__(&self) -> String {
        self.to_string()
    }
}

fn wavelet_handler(wavelet: PrismWavelet, events: PyObject) -> Result<(), PrismWavelet> {
    Python::with_gil(|py| {
        let py_wavelet = Wavelet::from_rust(py, &wavelet);
        if events.call1(py, (py_wavelet,)).is_err() {
            Err(wavelet)
        } else {
            Ok(())
        }
    })
}

#[pyclass]
struct Client {
    inner: PrismClient,
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(py: Python, addr: &str, event_callable: PyObject) -> PyResult<Self> {
        if !event_callable.as_ref(py).is_callable() {
            return Err(PyErr::new::<PyTypeError, _>("Must be a callable function"));
        }

        let uri = match addr.parse::<Uri>() {
            Ok(uri) => uri,
            Err(_) => return Err(PyErr::new::<PyValueError, _>("Invalid URI"))
        };
        let inner = PrismClient::connect(
            uri,
            move |wavelet: PrismWavelet| wavelet_handler(wavelet, event_callable.clone())
        );

        Ok(Self { inner })
    }

    pub fn add_beam(&mut self, beam: String) -> PyResult<()> {
        self.inner.add_beam(beam).map_err(|err|
            PyErr::new::<PyException, _>(err.to_string()))
    }

    pub fn transmissions(&mut self) -> PyResult<Vec<String>> {
        self.inner.transmissions().map_err(|err|
            PyErr::new::<PyException, _>(err.to_string()))
    }

    pub fn subscribe(&mut self, beam: String, index: Option<u64>) -> PyResult<()> {
        self.inner.subscribe(beam, index).map_err(|err|
            PyErr::new::<PyException, _>(err.to_string()))
    }

    pub fn unsubscribe(&mut self, beam: String) -> PyResult<()> {
        self.inner.unsubscribe(beam).map_err(|err|
            PyErr::new::<PyException, _>(err.to_string()))
    }

    pub fn emit(&mut self, beam: String, payload: &PyBytes) -> PyResult<()> {
        self.inner.emit(beam, payload.as_bytes().to_vec()).map_err(|err|
            PyErr::new::<PyException, _>(err.to_string()))
    }
}

/// This module is a python module implemented in Rust.
#[pymodule]
fn _prism(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Wavelet>()?;
    m.add_class::<Photon>()?;
    Ok(())
}
