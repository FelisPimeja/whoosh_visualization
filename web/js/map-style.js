export const routeLayerStyle = {
    id: 'routes',
    type: 'line',
    source: 'routes',
    'source-layer': 'routes_simplified',
    paint: {
        'line-color': [
            'interpolate',
            ['linear'],
            ['to-number', ['get', 'hour']],
            0, '#2c3e50',
            6, '#e74c3c',
            12, '#f39c12',
            18, '#3498db',
            23, '#2c3e50'
        ],
        'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            10, 0.2,
            14, 1
        ],
        'line-opacity': 0.3
    }
};

export const roadsUsageStyle = {
    id: 'roads-usage',
    type: 'line',
    source: 'roads-stat',
    'source-layer': 'roads_stat',
    paint: {
        'line-color': [
            'case',
            ['==', ['get', 'trip_count'], 0],
            '#cccccc',
            ['interpolate',
                ['linear'],
                ['get', 'trip_count'],
                10, '#ffeb3b',
                25, '#ffc107',
                50, '#ff9800',
                200,'#f44336'
            ]
        ],
        'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            10, [
                'case',
                ['==', ['get', 'trip_count'], 0],
                1,
                2
            ],
            14, [
                'case',
                ['==', ['get', 'trip_count'], 0],
                1,
                4
            ]
        ],
        'line-opacity': 0.8
    }
};

export const roadsSpeedStyle = {
    id: 'roads-speed',
    type: 'line',
    source: 'roads-stat',
    'source-layer': 'roads_stat',
    paint: {
        'line-color': [
            'case',
            ['==', ['get', 'med_speed'], 0],
            '#cccccc',
            ['interpolate',
                ['linear'],
                ['get', 'med_speed'],
                4, '#4caf50',   // Зелёный
                6, '#8bc34a',   // Светло-зелёный
                8, '#cddc39',   // Лаймовый
                12,'#ffc107',   // Янтарный
                16,'#f44336'    // Красный
            ]
        ],
        'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            10, [
                'case',
                ['==', ['get', 'med_speed'], 0],
                1,
                2
            ],
            14, [
                'case',
                ['==', ['get', 'med_speed'], 0],
                1,
                4
            ]
        ],
        'line-opacity': 0.8
    }
};


export const clustersLayerStyle = {
    id: 'clusters',
    type: 'circle',
    source: 'clusters',
    'source-layer': 'clusters',
    paint: {
        'circle-color': [
            'match',
            ['get', 'point_type'],
            'start', '#4285F4',
            'end', '#EA4335',
            '#9E9E9E'
        ],
        'circle-radius': [
            'interpolate',
            ['linear'],
            ['get', 'trip_count'],
            5, 4,
            100, 30
        ],
        'circle-stroke-width': [
            'case',
            ['boolean', ['feature-state', 'hover'], false],
            2,
            1
        ],
        'circle-stroke-color': '#ffffff',
        'circle-opacity': 0.6,
        'circle-stroke-opacity': [
            'case',
            ['boolean', ['feature-state', 'hover'], false],
            1,
            0.8
        ]
    }
};


export const mapStyle = 'https://api.maptiler.com/maps/dataviz-light/style.json';
